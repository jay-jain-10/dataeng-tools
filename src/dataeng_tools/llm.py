"""Thin wrapper around the Anthropic SDK (kept tiny so it is trivial to mock)."""
import anthropic

from .config import Config


class MissingKey(RuntimeError):
    pass


def run_agent(cfg: Config, system: str, user: str) -> str:
    if not cfg.api_key:
        raise MissingKey("No API key. Set ANTHROPIC_API_KEY or run `de config init` and edit the file.")
    client = anthropic.Anthropic(api_key=cfg.api_key)
    msg = client.messages.create(
        model=cfg.model,
        max_tokens=cfg.max_tokens,
        system=system,
        messages=[{"role": "user", "content": user}],
    )
    return "".join(b.text for b in msg.content if getattr(b, "type", "") == "text")
