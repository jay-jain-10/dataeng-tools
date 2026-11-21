# dataeng-tools

Claude-powered CLI for data engineers: semantic review, explanation and documentation of the things DEs actually ship — SQL, dbt, Airflow, Spark, warehouse and cloud configs.

> **Status: v0.1 (alpha).** Working CLI + 2 full agent prompts; the other 14 agents run from outlines and are being expanded.

## Idea
One `de` command, many **specialist agents**, each scoped to what a senior DE checks for that tool (e.g. the AWS agent knows Glue/EMR/Kinesis/Athena — not Route 53).

```bash
de review my_dag.py              # auto-detects airflow + snowflake, asks which agents to run
de review query.sql --opinionated
de explain query.sql
```

- **Balanced by default**, `--opinionated` for a firm recommendation — the user decides.
- **Cross-agent detection**: if several agents apply, the CLI asks before running them.
- **Severity levels**: CRITICAL / WARNING / INFO / SUGGESTION.

## Quickstart
```bash
pip install -e .            # from a clone (PyPI release pending)
export ANTHROPIC_API_KEY=sk-ant-...
de agents                   # list agents
de review examples/bad_pipeline.sql
de review dag.py            # detects airflow + snowflake -> asks which to run
de review dag.py --yes --format json   # CI-friendly
de explain query.sql
```
Flags: `--opinionated`, `--fix`, `--agent/-a NAME` (repeatable), `--all`, `--yes`, `--format json`.
Optional config: `de config init` -> `~/.dataeng/config.yaml` (model, warehouse/orchestrator/cloud hints).

## Repo layout
- `src/dataeng_tools/` — CLI (`cli.py`), agent loader (`agents.py`), tool detection (`detect.py`), config, LLM wrapper
- `tests/` — offline tests (LLM mocked); `examples/` — demo inputs
- `docs/DESIGN.md` — architecture, modes, severity framework, agent interaction rules
- `src/dataeng_tools/prompts/agents/*.md` — full system prompts (`sql-agent`, `aws-de-agent`)
- `src/dataeng_tools/prompts/agents/*.outline.md` — outlines for the remaining agents (python, dbt, airflow, dagster, snowflake, bigquery, clickhouse, redshift, gcp, azure, terraform, docker, incident, data-quality)

## Contributing
Prompts are plain Markdown — PRs that sharpen a check, add a dialect gotcha, or expand an outline into a full prompt are the most valuable contribution.

## License
MIT
