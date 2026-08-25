"""
Azure OpenAI client for the lead-qualification pipeline.

Deliberately free of Django imports: the standalone scripts in scripts/ run on the server against
a checkout whose models may not match, so they need to import this without pulling in settings.
Config comes from the environment, falling back to reading .env directly.

Why this module exists at all: the qualification prompt is used from four places (a management
command, two standalone scripts, and the batch pipeline). Each previously built its own Anthropic
client, so switching provider meant four edits and four chances to miss one.
"""
import json
import logging
import os
import pathlib
import re

logger = logging.getLogger(__name__)

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[3]

# Strip a ```json fence if the model adds one. With response_format=json_object it should not,
# but the parameter is silently ignored on some api-versions and the fence comes back.
_FENCE_RE = re.compile(r"^```(?:json)?\s*|\s*```$")


def _env(name: str, default: str = "") -> str:
    val = os.environ.get(name)
    if val:
        return val
    # Fall back to .env so standalone scripts work without a shell export.
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


def deployment() -> str:
    return _env("AZURE_OPENAI_DEPLOYMENT", "production-gpt-4.1")


# Approximate published GPT-4.1 list pricing (USD per 1M tokens) -- Azure's actual billed rate
# may differ (region/negotiated pricing), so these are overridable via env rather than asserted
# as exact. Good enough for a pre-call sanity estimate, not for real accounting.
_INPUT_PRICE_PER_1M = float(_env("AZURE_OPENAI_INPUT_PRICE_PER_1M", "2.00"))
_OUTPUT_PRICE_PER_1M = float(_env("AZURE_OPENAI_OUTPUT_PRICE_PER_1M", "8.00"))


def estimate_tokens(text: str) -> int:
    """Rough chars/4 heuristic (the standard rule of thumb for GPT-family tokenizers) -- no
    tiktoken dependency, since this module is deliberately dependency-free (see module
    docstring). Good enough for a cost sanity check, not exact billing."""
    return max(1, len(text) // 4)


def estimate_cost(system: str, user: str, max_tokens: int) -> dict:
    """
    Pre-call cost estimate. max_tokens is used as the OUTPUT token count -- an upper bound, not
    a prediction of actual usage, since actual output is usually well under the cap. Call this
    before complete_json() when the input is large/variable enough that cost isn't obviously
    trivial (e.g. one call per brand with different-sized prompts), not for every single call.
    """
    input_tokens = estimate_tokens(system) + estimate_tokens(user)
    input_cost = input_tokens / 1_000_000 * _INPUT_PRICE_PER_1M
    output_cost = max_tokens / 1_000_000 * _OUTPUT_PRICE_PER_1M
    return {
        "input_tokens_est": input_tokens,
        "output_tokens_max": max_tokens,
        "input_cost_usd": round(input_cost, 4),
        "output_cost_usd_worst_case": round(output_cost, 4),
        "total_cost_usd_worst_case": round(input_cost + output_cost, 4),
    }


def client(max_retries: int = 6, timeout: float = 120.0):
    """
    Azure OpenAI client.

    timeout matters as much as max_retries: the SDK default is 600s per attempt, so a single
    hung connection parks a worker for ten minutes and, with retries, well over an hour. A bulk
    run with N workers can stall completely while the endpoint itself is perfectly healthy --
    observed on the RealTruck prioritisation run, where all 5 workers sat on open sockets to
    Azure for 40+ minutes. 120s is far above the ~3s a real call takes.
    """
    from openai import AzureOpenAI

    endpoint = _env("AZURE_OPENAI_ENDPOINT")
    api_key = _env("AZURE_OPENAI_API_KEY")
    if not endpoint or not api_key:
        raise RuntimeError("AZURE_OPENAI_ENDPOINT / AZURE_OPENAI_API_KEY are not set")
    return AzureOpenAI(
        azure_endpoint=endpoint,
        api_key=api_key,
        api_version=_env("AZURE_OPENAI_API_VERSION", "2024-06-01"),
        max_retries=max_retries,
        timeout=timeout,
    )


def complete_json(cli, system: str, user: str, max_tokens: int = 1024,
                  model: str | None = None) -> tuple[dict | None, str | None]:
    """
    One JSON-returning completion. Returns (parsed, error) -- never raises, so a single bad
    lead cannot kill a bulk run.
    """
    try:
        resp = cli.chat.completions.create(
            model=model or deployment(),
            messages=[{"role": "system", "content": system},
                      {"role": "user", "content": user}],
            max_tokens=max_tokens,
            temperature=0,
            response_format={"type": "json_object"},
        )
        if resp.usage:
            # Real usage, as opposed to estimate_cost()'s pre-call guess -- lets a caller compare
            # what it budgeted (max_tokens) against what actually got used, instead of assuming
            # the worst-case ceiling was hit every time.
            logger.info("azure_llm real usage: prompt_tokens=%s completion_tokens=%s (max_tokens budget was %s)",
                        resp.usage.prompt_tokens, resp.usage.completion_tokens, max_tokens)
        raw = _FENCE_RE.sub("", (resp.choices[0].message.content or "").strip())
        return json.loads(raw), None
    except json.JSONDecodeError as e:
        return None, f"JSON parse error: {e}"
    except Exception as e:
        return None, f"{type(e).__name__}: {e}"[:250]
