"""
Self-hosted Qwen client, mirroring azure_llm.py's interface (client() / complete_json()) so
product_grouping.py and classification.py can swap providers by passing a different client,
with no changes to their own code.

Talks to whatever serves the model (vLLM, Ollama, text-generation-webui, LM Studio, ...) via
the standard OpenAI-compatible /chat/completions API, which all of those implement -- so this
module doesn't need to know or care which one is actually running on the host machine. Point
QWEN_API_BASE_URL at it and this works.
"""
import json
import os
import pathlib
import re

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[3]

_FENCE_RE = re.compile(r"^```(?:json)?\s*|\s*```$")


def _env(name: str, default: str = "") -> str:
    val = os.environ.get(name)
    if val:
        return val
    try:
        for line in (_REPO_ROOT / ".env").read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                if k.strip() == name:
                    return v.strip().strip('"').strip("'")
    except OSError:
        pass
    return default


def model_name() -> str:
    return _env("QWEN_MODEL_NAME", "qwen2.5")


def client(timeout: float = 300.0, max_retries: int = 3):
    """
    OpenAI SDK pointed at the self-hosted endpoint. timeout is much higher than azure_llm's
    120s default -- a single-GPU/CPU-served model doing a 24,000-token-budget Stage D call is
    realistically slower than a hosted multi-tenant API, so a tight timeout would abort real
    in-flight generations rather than catch actually-hung connections. Tune down once real
    per-call latency on the host machine is known.
    """
    from openai import OpenAI

    base_url = _env("QWEN_API_BASE_URL")
    if not base_url:
        raise RuntimeError("QWEN_API_BASE_URL is not set -- point it at the host machine's OpenAI-compatible endpoint")
    return OpenAI(
        base_url=base_url,
        # Most self-hosted servers don't check the key at all, but the SDK requires a non-empty
        # string -- QWEN_API_KEY lets you set a real one if the host machine is locked down.
        api_key=_env("QWEN_API_KEY", "not-needed"),
        max_retries=max_retries,
        timeout=timeout,
    )


def complete_json(cli, system: str, user: str, max_tokens: int = 1024,
                   model: str | None = None) -> tuple[dict | None, str | None]:
    """
    Same contract as azure_llm.complete_json: (parsed, error), never raises. Also logs REAL
    prompt/completion token usage from the response -- the thing azure_llm.py never captured,
    leaving every cost figure this session as a worst-case estimate rather than a measurement.
    Self-hosted OpenAI-compatible servers return usage the same way Azure does, so this is free
    to get right from the start here.
    """
    import logging
    logger = logging.getLogger(__name__)
    try:
        resp = cli.chat.completions.create(
            model=model or model_name(),
            messages=[{"role": "system", "content": system},
                      {"role": "user", "content": user}],
            max_tokens=max_tokens,
            temperature=0,
            response_format={"type": "json_object"},
        )
        if resp.usage:
            logger.info("qwen_llm real usage: prompt_tokens=%s completion_tokens=%s (max_tokens budget was %s)",
                        resp.usage.prompt_tokens, resp.usage.completion_tokens, max_tokens)
        raw = _FENCE_RE.sub("", (resp.choices[0].message.content or "").strip())
        return json.loads(raw), None
    except json.JSONDecodeError as e:
        return None, f"JSON parse error: {e}"
    except Exception as e:
        return None, f"{type(e).__name__}: {e}"[:250]
