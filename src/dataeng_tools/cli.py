"""`de` command line interface."""
import json
import sys
from pathlib import Path

import click
from rich.console import Console
from rich.markdown import Markdown

from . import __version__, agents as agent_registry
from .config import init_config, load_config
from .detect import detect
from .llm import MissingKey, run_agent

console = Console()
MAX_BYTES = 200_000


def _read(path: str) -> tuple[Path, str]:
    p = Path(path)
    if not p.is_file():
        raise click.ClickException(f"File not found: {path}")
    if p.stat().st_size > MAX_BYTES:
        raise click.ClickException(f"{path} is larger than {MAX_BYTES // 1000} KB; review a smaller file or split it.")
    return p, p.read_text(errors="replace")


def _choose_agents(detected: list[str], explicit: tuple[str, ...], yes: bool, run_all: bool) -> list[str]:
    known = agent_registry.list_agents()
    if explicit:
        bad = [a for a in explicit if a not in known]
        if bad:
            raise click.ClickException(f"Unknown agent(s): {', '.join(bad)}. Try `de agents`.")
        return list(explicit)
    if not detected:
        raise click.ClickException("Couldn't detect a relevant agent. Pass one with --agent (see `de agents`).")
    if len(detected) == 1 or run_all:
        return detected
    if yes or not sys.stdin.isatty():
        return detected
    click.echo(f"Detected: {', '.join(detected)}")
    click.echo("  [a] run all (merged output)")
    for i, a in enumerate(detected, 1):
        click.echo(f"  [{i}] {a} only")
    click.echo("  [q] cancel")
    ans = click.prompt("Choose", default="a").strip().lower()
    if ans == "q":
        raise click.Abort()
    if ans == "a":
        return detected
    if ans.isdigit() and 1 <= int(ans) <= len(detected):
        return [detected[int(ans) - 1]]
    raise click.ClickException("Invalid choice.")


def _run(path: str, mode: str, agent: tuple[str, ...], yes: bool, run_all: bool, fix: bool, fmt: str):
    cfg = load_config()
    p, text = _read(path)
    chosen = _choose_agents(detect(p, text), agent, yes, run_all)
    results = {}
    for name in chosen:
        system = agent_registry.get_prompt(name, mode=mode, fix=fix, context=cfg.context_hint())
        user = f"File: {p.name}\n\n```\n{text}\n```"
        try:
            with console.status(f"[bold]{name}[/] reviewing...") if fmt == "terminal" else _null():
                results[name] = run_agent(cfg, system, user)
        except MissingKey as e:
            raise click.ClickException(str(e))
    if fmt == "json":
        click.echo(json.dumps({"file": str(p), "mode": mode, "agents": results}, indent=2))
        return
    for name, out in results.items():
        console.rule(f"[bold cyan]{name}")
        console.print(Markdown(out))
    if len(results) > 1:
        console.rule("[bold]Merged: agents ran independently; review each section for overlap or conflicts")


class _null:
    def __enter__(self): return self
    def __exit__(self, *a): return False


def common_options(f):
    f = click.option("--agent", "-a", multiple=True, help="Force specific agent(s). See `de agents`.")(f)
    f = click.option("--all", "run_all", is_flag=True, help="Run every detected agent without prompting.")(f)
    f = click.option("--yes", "-y", is_flag=True, help="Non-interactive: run all detected agents (CI).")(f)
    f = click.option("--format", "fmt", type=click.Choice(["terminal", "json"]), default="terminal")(f)
    return f


@click.group()
@click.version_option(__version__)
def main():
    """dataeng-tools: specialist Claude agents for data engineers."""


@main.command()
@click.argument("path")
@click.option("--opinionated", is_flag=True, help="Give firm recommendations instead of balanced trade-offs.")
@click.option("--fix", is_flag=True, help="Also output a corrected version.")
@common_options
def review(path, opinionated, fix, agent, run_all, yes, fmt):
    """Review a file (SQL, DAG, dbt model, Terraform, Dockerfile, ...)."""
    _run(path, "opinionated" if opinionated else "balanced", agent, yes, run_all, fix, fmt)


@main.command()
@click.argument("path")
@common_options
def explain(path, agent, run_all, yes, fmt):
    """Explain what a file does in plain English."""
    _run(path, "explain", agent, yes, run_all, False, fmt)


@main.command()
def agents():
    """List available agents."""
    for name, kind in agent_registry.list_agents().items():
        click.echo(f"{name:22} {'full prompt' if kind == 'full' else 'outline-based'}")


@main.group()
def config():
    """Manage configuration."""


@config.command("init")
def config_init():
    """Create ~/.dataeng/config.yaml."""
    click.echo(f"Config at {init_config()}")
