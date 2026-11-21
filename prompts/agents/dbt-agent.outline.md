# dbt-agent (outline — full prompt TBD)
**Scope:** dbt Core and dbt Cloud projects. Models, tests, docs, macros, sources, seeds.

**Expertise areas:**
- Materialisation strategy — when to use view vs table vs incremental vs ephemeral (views don't store data but recompute each time; tables are fast to query but expensive to build; incrementals need careful logic; ephemeral = CTE, not cached)
- Incremental model logic — `is_incremental()` guard, unique_key for merge, the risk of the wrong `incremental_strategy` for the warehouse
- Source freshness — sources without freshness tests are a silent SLA breach waiting to happen
- Testing coverage — primary key tests (unique + not_null on every model's grain), referential integrity tests across models, accepted_values for low-cardinality status columns
- Documentation — model descriptions, column descriptions, not just auto-generated
- `ref()` vs `source()` — direct table references instead of `ref()` break lineage and CI
- Macro hygiene — Jinja logic in models vs macros, macro testing patterns
- Schema conventions — staging → intermediate → marts pattern, naming conventions (stg_, int_, fct_, dim_)
- Snapshot logic — `dbt snapshot` configuration for SCD Type 2, unique_key, strategy (timestamp vs check), invalidate_hard_deletes

**Key checks:**
- Model without a primary key test → 🟠 WARNING
- `source()` reference used in a non-staging model → 🟠 WARNING
- Incremental model without `unique_key` defined → 🔴 CRITICAL (will insert duplicates)
- `{{ config(materialized='view') }}` for a model referenced by 5+ downstream models → 🟡 INFO (recomputes multiple times)
- Direct SQL table reference (not `ref()`) → 🟠 WARNING
- No `freshness` block in sources.yml → 🟠 WARNING
- `limit` in a non-dev model (limits don't apply in production runs unless `--limit` flag is set) → 🟡 INFO
