"""Agent registry: loads full prompts, or wraps an outline into a working prompt."""
from importlib import resources

SEVERITY_BLOCK = """\
SEVERITY: use 🔴 CRITICAL (wrong results / data loss / prod failure), 🟠 WARNING (degrades at scale),
🟡 INFO (will matter at 10x), 🟢 SUGGESTION (best practice).

OUTPUT FORMAT for each finding:
[severity] [title]
Location: ...
Issue: ...
Risk: ...
Fix: ... (show corrected code where possible)
End with: 'Found: X critical · Y warnings · Z info · W suggestions' and a one-sentence verdict."""

OUTLINE_WRAPPER = """\
You are the {name} inside dataeng-tools, a CLI for data engineers. Review the input as a senior data
engineer with 8+ years of production experience would. Stay strictly within the scope below; do not
comment on out-of-scope topics. Only report issues actually present in the input.

{outline}

{severity}
"""

MODE_SUFFIX = {
    "balanced": (
        "MODE: balanced (default). Present trade-offs and explain when each option fits. "
        "Do not push a single style or architecture on the user; flag context-dependent choices as such."
    ),
    "opinionated": (
        "MODE: opinionated. Lead each finding with a direct recommendation using decisive language "
        "('Change X to Y'). State trade-offs briefly, but commit to a choice."
    ),
    "explain": (
        "MODE: explain. Skip severity/fix structure. Walk through what the input does, section by section, "
        "in plain English, then note concerns conversationally at the end."
    ),
}
FIX_SUFFIX = "ALSO: after the findings, output a corrected version of the input in a single fenced code block."


def _agents_dir():
    return resources.files("dataeng_tools").joinpath("prompts", "agents")


def list_agents() -> dict[str, str]:
    """name -> 'full' | 'outline'"""
    out: dict[str, str] = {}
    for p in sorted(_agents_dir().iterdir(), key=lambda x: x.name):
        n = p.name
        if n.endswith(".outline.md"):
            out.setdefault(n[: -len(".outline.md")], "outline")
        elif n.endswith(".md"):
            out[n[: -len(".md")]] = "full"
    return out


def get_prompt(name: str, mode: str = "balanced", fix: bool = False, context: str = "") -> str:
    kinds = list_agents()
    if name not in kinds:
        raise KeyError(f"Unknown agent '{name}'. Available: {', '.join(kinds)}")
    d = _agents_dir()
    if kinds[name] == "full":
        base = d.joinpath(f"{name}.md").read_text()
    else:
        outline = d.joinpath(f"{name}.outline.md").read_text()
        base = OUTLINE_WRAPPER.format(name=name, outline=outline, severity=SEVERITY_BLOCK)
    parts = [base, MODE_SUFFIX[mode]]
    if fix:
        parts.append(FIX_SUFFIX)
    if context:
        parts.append(context)
    return "\n\n".join(parts)
