# dataeng-tools

Claude-powered CLI for data engineers: semantic review, explanation and documentation of the things DEs actually ship — SQL, dbt, Airflow, Spark, warehouse and cloud configs.

> **Status: v0.1 (alpha).** Working CLI and 16 full agent prompts (researched against current docs, independently reviewed). Prompts are not yet validated against real-world files at scale: feedback and PRs welcome.

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
- `src/dataeng_tools/prompts/agents/*.md` — the 16 agent system prompts: sql, python, dbt, airflow, dagster, snowflake, bigquery, clickhouse, redshift, aws-de, gcp-de, azure-de, terraform-de, docker-de, incident, data-quality

## Contributing
Prompts are plain Markdown — PRs that sharpen a check, add a dialect gotcha, or tighten a prompt that over- or under-reports are the most valuable contribution.

## License
MIT

## Security notes
- File contents are sent to the Anthropic API. Don't review files containing secrets or data you can't share.
- Input is wrapped in a delimited block and every prompt tells the model to treat it as untrusted data. Output is advisory: **don't use it as a pass/fail gate** in CI; a crafted file may still influence it.
- Prefer `ANTHROPIC_API_KEY` over storing the key in `~/.dataeng/config.yaml`.
