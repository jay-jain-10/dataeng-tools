# dataeng-tools

Claude-powered CLI for data engineers: semantic review, explanation and documentation of the things DEs actually ship — SQL, dbt, Airflow, Spark, warehouse and cloud configs.

> **Status: v0.1 — design phase.** The agent prompts are the product. CLI code comes next.

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

## Repo layout
- `docs/DESIGN.md` — architecture, modes, severity framework, agent interaction rules
- `prompts/agents/*.md` — full system prompts (`sql-agent`, `aws-de-agent`)
- `prompts/agents/*.outline.md` — outlines for the remaining agents (python, dbt, airflow, dagster, snowflake, bigquery, clickhouse, redshift, gcp, azure, terraform, docker, incident, data-quality)

## Contributing
Prompts are plain Markdown — PRs that sharpen a check, add a dialect gotcha, or expand an outline into a full prompt are the most valuable contribution.

## License
MIT
