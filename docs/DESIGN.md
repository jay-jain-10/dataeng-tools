# dataeng-tools — Design

## Part 1 — Architecture Overview

### Agent Map

```
dataeng-tools
│
├── Language / Query
│   ├── sql-agent          Dialect-aware SQL review & explanation
│   ├── python-agent       DE-scoped Python (PySpark, pandas, pipeline code)
│   └── dbt-agent          dbt models, tests, docs, macros, incremental logic
│
├── Orchestration
│   ├── airflow-agent      DAGs, operators, sensors, XCom, scheduling
│   └── dagster-agent      Assets, jobs, ops, partitions, resources
│
├── Warehouses & Storage
│   ├── snowflake-agent    Virtual WH, clustering, Snowpipe, streams, tasks
│   ├── bigquery-agent     Slots, partitioning, clustering, cost, streaming
│   ├── clickhouse-agent   MergeTree engines, ordering keys, MVs, sharding
│   └── redshift-agent     Distribution styles, sort keys, WLM, COPY
│
├── Cloud (DE-scoped only)
│   ├── aws-de-agent       Glue, EMR, Kinesis, S3, Athena, LakeFormation, MSK
│   ├── gcp-de-agent       Dataflow, Dataproc, Pub/Sub, GCS, Dataplex
│   └── azure-de-agent     ADF, Synapse, Event Hubs, ADLS, Databricks
│
├── Infrastructure (DE-scoped)
│   ├── terraform-de-agent Data infra as code, provider patterns
│   └── docker-de-agent    Pipeline containers, Spark/Airflow images
│
└── Cross-cutting
    ├── incident-agent     Log triage, failure diagnosis, postmortem drafting
    └── data-quality-agent GX, dbt tests, Soda, contracts, SLA monitoring
```

---

### Cross-Agent Detection

When a user runs `de review <file>`, the CLI scans the file for tool signatures before invoking any agent. If multiple agents are relevant, it prompts:

```
Detected: Airflow DAG with SnowflakeOperator
→ Run airflow-agent only?     [1]
→ Run snowflake-agent only?   [2]
→ Run both and merge output?  [3] (recommended)
→ Cancel                      [q]
```

Detection is heuristic-based:
- Import statements (`from airflow`, `import pyspark`)
- SQL dialect keywords (`VARIANT`, `QUALIFY` → Snowflake; `ARRAY_AGG`, `UNNEST` → BigQuery)
- File extensions (`.tf` → terraform-de-agent, `dbt_project.yml` → dbt-agent)
- Config keys (`executor: KubernetesExecutor` → airflow-agent)

When agents are merged, each produces its own findings section. No cross-contamination of scope.

---

### Mode Flags

| Flag | Behaviour |
|------|-----------|
| *(default)* | Balanced. Presents tradeoffs, explains options, avoids prescribing one answer. |
| `--opinionated` | Gives a direct recommendation with explicit reasoning. States trade-offs briefly but commits to a choice. |
| `--explain` | Prioritises plain-English explanation over review findings. Best for understanding unfamiliar code. |
| `--fix` | Attempts to produce a corrected version of the input alongside findings. |
| `--format json` | Returns structured JSON instead of terminal output. For CI/CD integration. |

---

### Severity Framework (used by all agents)

| Level | Meaning | Example |
|-------|---------|---------|
| `🔴 CRITICAL` | Will cause incorrect results, data loss, or production failure | Non-idempotent INSERT in a scheduled job |
| `🟠 WARNING` | Will cause performance degradation or technical debt at scale | Missing retry logic on an API task |
| `🟡 INFO` | Good to know; won't bite you today but will at 10x scale | SELECT * in a CTE that's referenced 4 times |
| `🟢 SUGGESTION` | Best practice / style / community convention | Missing dbt model description |

---

### Universal Prompt Template Schema

Every agent's system prompt follows this structure:

```
IDENTITY       — what this agent is and its precise scope
OUT OF SCOPE   — what it explicitly does NOT cover (enforced hard boundary)
EXPERTISE      — the specific knowledge areas it draws from
REVIEW LOGIC   — what to check, in what order, with what severity
MODE HANDLING  — how to adjust output for --opinionated vs balanced
OUTPUT FORMAT  — structure of the response
DIALECT NOTES  — (for agents that are multi-tool aware)
```

---

---


## Part 4 — Cross-Agent Interaction Rules

When multiple agents are merged (user selects option 3 at the prompt):

1. Each agent runs its full review independently — no shared state
2. Findings are sectioned by agent in the output (not interleaved)
3. Conflicting recommendations are flagged explicitly:
   ```
   ⚠️  AGENT CONFLICT: airflow-agent recommends using a KubernetesPodOperator
   for isolation, but docker-de-agent notes the base image lacks pinned
   dependencies. Resolve the image issue first.
   ```
4. A merged SUMMARY appears at the end covering all agents combined

---

## Part 5 — Open Questions for Community Input

These are intentionally left open for community contribution once the project is public:

1. **Databricks agent** — enough overlap with Spark + cloud agents, or distinct enough to warrant its own? (Likely yes — Unity Catalog, Photon, Delta Live Tables are unique)
2. **Kafka agent (self-managed)** — separate from MSK agent for on-premise/Confluent Cloud users
3. **dbt adapters** — should dbt-agent be aware of adapter-specific behaviours (dbt-snowflake vs dbt-bigquery vs dbt-spark)?
4. **Prompt versioning** — how do we version prompts as tools evolve (dbt 1.8 → 1.9 introduced breaking changes to incremental logic)?
5. **Language support** — Scala for Spark? SQL-only vs Python+SQL for the same review?
