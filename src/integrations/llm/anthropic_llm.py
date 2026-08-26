"""
Anthropic client, mirroring ``azure_llm``'s ``client()`` / ``complete_json()`` interface so a
caller can swap providers by passing a different module.

Exists for **independent verification**, not for production enrichment. Re-running the same
prompt through the same model measures reproducibility, not correctness: the errors are
correlated, so a confidently-wrong answer comes back confidently wrong. Auditing with a different
vendor's model gives errors that are at least plausibly independent, which is what makes an
agreement rate mean something.

Deliberately free of Django imports, like ``azure_llm`` -- config comes from the environment,
falling back to reading ``.env`` directly.
"""
import json
import logging
import typing

from src.integrations.llm.azure_llm import _FENCE_RE, _env

logger = logging.getLogger(__name__)

DEFAULT_MODEL = "claude-sonnet-4-5-20250929"

# Published list pricing (USD per 1M tokens) for the default model, overridable via env. Used for
# a pre-run cost estimate only, not for accounting.
_INPUT_PRICE_PER_1M = float(_env("ANTHROPIC_INPUT_PRICE_PER_1M", "3.00"))
_OUTPUT_PRICE_PER_1M = float(_env("ANTHROPIC_OUTPUT_PRICE_PER_1M", "15.00"))


def model_name() -> str:
    return _env("ANTHROPIC_AUDIT_MODEL", DEFAULT_MODEL)


def price_per_call(input_tokens: int, output_tokens: int) -> float:
    return input_tokens / 1_000_000 * _INPUT_PRICE_PER_1M + output_tokens / 1_000_000 * _OUTPUT_PRICE_PER_1M


def _first_json_object(text: str) -> str:
    """
    The first balanced ``{...}`` in the text.

    Without ``response_format`` this model will sometimes add a sentence after the JSON, which
    made json.loads fail with "Extra data" on 15% of audit calls -- and a failed call is a
    silently dropped sample, which biases the very measurement the audit exists to produce.
    Brace-matching rather than a regex because the payload nests.
    """
    start = text.find("{")
    if start == -1:
        return text
    depth = 0
    in_string = False
    escaped = False
    for index in range(start, len(text)):
        char = text[index]
        if in_string:
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif char == '"':
                in_string = False
            continue
        if char == '"':
            in_string = True
        elif char == "{":
            depth += 1
        elif char == "}":
            depth -= 1
            if depth == 0:
                return text[start : index + 1]
    return text[start:]


def client(timeout: float = 120.0, max_retries: int = 4):
    import anthropic

    api_key = _env("ANTHROPIC_API_KEY")
    if not api_key:
        raise RuntimeError("ANTHROPIC_API_KEY is not set")
    return anthropic.Anthropic(api_key=api_key, timeout=timeout, max_retries=max_retries)


def complete_json(
    cli,
    system: str,
    user: str,
    max_tokens: int = 1024,
    model: typing.Optional[str] = None,
) -> typing.Tuple[typing.Optional[dict], typing.Optional[str]]:
    """
    One JSON-returning completion. Returns ``(parsed, error)`` and never raises, so one bad row
    cannot kill an audit run -- same contract as ``azure_llm.complete_json``.

    There is no ``response_format`` equivalent here, so the fence stripper is doing real work
    rather than being defensive: this model does wrap JSON in a ```json block.
    """
    try:
        response = cli.messages.create(
            model=model or model_name(),
            max_tokens=max_tokens,
            temperature=0,
            system=system,
            messages=[{"role": "user", "content": user}],
        )
        text = "".join(block.text for block in response.content if getattr(block, "type", None) == "text")
        return json.loads(_first_json_object(_FENCE_RE.sub("", text.strip()))), None
    except json.JSONDecodeError as exc:
        return None, "JSON parse error: {}".format(exc)
    except Exception as exc:
        return None, "{}: {}".format(type(exc).__name__, exc)[:250]
