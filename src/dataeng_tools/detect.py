"""Heuristic tool detection: which agents are relevant to this file?"""
import re
from pathlib import Path

# (agent, regex) — any match counts; weight = number of distinct hits
RULES: dict[str, list[str]] = {
    "airflow-agent": [r"\bapache/airflow\b", r"\bfrom airflow\b", r"\bimport airflow\b", r"\bDAG\(", r"@dag\b", r"\bXCom\b", r"\bBashOperator\b", r"\bPythonOperator\b"],
    "dagster-agent": [r"\bfrom dagster\b", r"\bimport dagster\b", r"@asset\b", r"@op\b", r"@job\b"],
    "dbt-agent": [r"\{\{\s*(ref|source|config)\(", r"\bis_incremental\(\)", r"materialized\s*=", r"^\s*models:\s*$"],
    "snowflake-agent": [r"\bQUALIFY\b", r"\bVARIANT\b", r"\bFLATTEN\(", r"\bSnowflake\w*Operator\b", r"\bCOPY INTO\b",
                        r"\bsnowflake\.connector\b", r"\bsnowflake_", r"\bWAREHOUSE_SIZE\b"],
    "bigquery-agent": [r"\bgoogle_bigquery\w*", r"\bUNNEST\(", r"\bARRAY_AGG\(", r"\bBigQuery\w*Operator\b", r"\bgoogle\.cloud\.bigquery\b",
                       r"`[\w-]+\.[\w-]+\.[\w-]+`", r"\bINFORMATION_SCHEMA\.JOBS"],
    "clickhouse-agent": [r"\bMergeTree\b", r"\barrayJoin\(", r"\bclickhouse", r"\bgroupArray\("],
    "redshift-agent": [r"\bDISTKEY\b", r"\bSORTKEY\b", r"\bDISTSTYLE\b", r"\bUNLOAD\s*\(", r"\bRedshift\w*Operator\b"],
    "python-agent": [r"\bimport pyspark\b", r"\bfrom pyspark\b", r"\bimport pandas\b", r"\.collect\(\)", r"\.toPandas\(\)"],
    "aws-de-agent": [r"\bboto3\b", r"\baws_glue\b", r"\baws_(s3|glue|emr|kinesis|athena|msk|lakeformation)\w*",
                     r"\bGlue\w*Operator\b", r"\bEmr\w*Operator\b", r"\bs3://", r"\bawsglue\b"],
    "gcp-de-agent": [r"\bgs://", r"\bgoogle_(bigquery|storage|pubsub|dataflow|dataproc|composer)\w*", r"\bDataflow\w*Operator\b", r"\bDataproc\w*Operator\b", r"\bgoogle_(pubsub|dataflow|dataproc)\w*"],
    "azure-de-agent": [r"\babfss://", r"\bDataFactory\w*", r"\bazurerm_(data_factory|synapse|eventhub|storage)\w*"],
    "terraform-de-agent": [r"^\s*resource\s+\"", r"^\s*provider\s+\"", r"^\s*terraform\s*\{"],
    "docker-de-agent": [r"^\s*FROM\s+\S+", r"^\s*(RUN|COPY|ENTRYPOINT)\s", r"^\s*services:\s*$"],
}

SQL_HINT = re.compile(r"\b(select|insert\s+into|merge\s+into|create\s+(or\s+replace\s+)?(table|view))\b", re.I)
LOGLIKE = re.compile(r"(Traceback \(most recent call last\)|Exception in thread|^\S*\d{4}-\d\d-\d\d[T ][\d:.,]+\S*\s+(ERROR|FATAL|CRITICAL)\b|^\[?(ERROR|FATAL)\]?[: ])", re.M)

EXT_DEFAULTS = {".sql": "sql-agent", ".tf": "terraform-de-agent", "Dockerfile": "docker-de-agent", ".log": "incident-agent"}


def detect(path: Path, text: str) -> list[str]:
    if path.suffix.lower() in {".md", ".txt", ".rst"}:
        return []  # prose: mentions of tools are not usage; pass --agent explicitly
    scores: dict[str, int] = {}
    for agent, pats in RULES.items():
        n = sum(1 for p in pats if re.search(p, text, re.M | re.I))
        if n:
            scores[agent] = n
    found = [a for a, _ in sorted(scores.items(), key=lambda kv: -kv[1])]

    # SQL is the baseline whenever real SQL is present (including inside Python / dbt files)
    if "airflow-agent" in found and not re.search(r"\b(from|import) airflow\b|apache/airflow", text):
        found.remove("airflow-agent")
    if SQL_HINT.search(text) and "sql-agent" not in found:
        found.insert(0, "sql-agent") if path.suffix == ".sql" else found.append("sql-agent")
    if LOGLIKE.search(text) or path.suffix == ".log":
        found.insert(0, "incident-agent")

    default = EXT_DEFAULTS.get(path.suffix) or EXT_DEFAULTS.get(path.name)
    if default and default not in found:
        found.insert(0, default)

    # a bare Dockerfile/compose heuristic is noisy inside other file types: keep only if file looks like one
    is_docker = path.name.startswith("Dockerfile") or path.suffix == ".dockerfile" or "compose" in path.name
    if is_docker and "docker-de-agent" not in found:
        found.insert(0, "docker-de-agent")
    if "docker-de-agent" in found and not is_docker:
        found.remove("docker-de-agent")
    if "terraform-de-agent" in found and path.suffix != ".tf":
        found.remove("terraform-de-agent")

    seen, out = set(), []
    for a in found:
        if a not in seen:
            seen.add(a)
            out.append(a)
    return out
