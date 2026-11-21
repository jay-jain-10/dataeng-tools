"""Thin wrapper around the Anthropic SDK (kept tiny so it is trivial to mock)."""
import anthropic

from .config import Config


class MissingKey(RuntimeError):
    pass


class LLMError(RuntimeError):
    pass


def run_agent(cfg: Config, system: str, user: str) -> str:
    if not cfg.api_key:
        raise MissingKey("No API key. Set ANTHROPIC_API_KEY or run `de config init` and edit the file.")
    client = anthropic.Anthropic(api_key=cfg.api_key, max_retries=3, timeout=180.0)
    try:
        msg = client.messages.create(
            model=cfg.model, max_tokens=cfg.max_tokens, system=system,
            messages=[{"role": "user", "content": user}],
        )
    except anthropic.AuthenticationError:
        raise LLMError("Authentication failed: check your ANTHROPIC_API_KEY.")
    except anthropic.RateLimitError:
        raise LLMError("Rate limited by the API after retries; try again shortly.")
    except anthropic.APIStatusError as e:
        raise LLMError(f"API error {e.status_code}: {getattr(e, 'message', e)}")
    except anthropic.APIConnectionError as e:
        raise LLMError(f"Could not reach the API: {e}")
    text = "".join(b.text for b in msg.content if getattr(b, "type", "") == "text")
    if getattr(msg, "stop_reason", None) == "max_tokens":
        text += "\n\n> Output truncated at max_tokens; raise `max_tokens` in your config."
    return text
