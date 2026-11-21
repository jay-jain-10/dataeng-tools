from pathlib import Path

from click.testing import CliRunner

from dataeng_tools import agents, cli
from dataeng_tools.detect import detect

AIRFLOW_SNOWFLAKE = '''
from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
t = SnowflakeOperator(task_id="x", sql="INSERT INTO t SELECT * FROM s QUALIFY row_number() over (order by 1)=1")
'''


def test_all_agents_load_and_modes():
    names = agents.list_agents()
    assert {"sql-agent", "aws-de-agent", "dbt-agent", "airflow-agent"} <= set(names)
    for n in names:
        for mode in ("balanced", "opinionated", "explain"):
            assert mode.upper() in agents.get_prompt(n, mode=mode).upper()


def test_outline_agent_is_wrapped():
    p = agents.get_prompt("dbt-agent")
    assert "dbt-agent inside dataeng-tools" in p and "SEVERITY" in p


def test_detect_airflow_snowflake():
    found = detect(Path("dag.py"), AIRFLOW_SNOWFLAKE)
    assert "airflow-agent" in found and "snowflake-agent" in found and "sql-agent" in found


def test_detect_sql_file_and_docker_noise():
    assert detect(Path("q.sql"), "select 1")[0] == "sql-agent"
    assert "docker-de-agent" not in detect(Path("x.py"), "FROM foo\nRUN x")


def test_cli_review_multi_agent_json(tmp_path, monkeypatch):
    f = tmp_path / "dag.py"
    f.write_text(AIRFLOW_SNOWFLAKE)
    monkeypatch.setattr(cli, "run_agent", lambda cfg, system, user: "FINDING")
    r = CliRunner().invoke(cli.main, ["review", str(f), "--yes", "--format", "json", "--opinionated"])
    assert r.exit_code == 0, r.output
    assert '"airflow-agent": "FINDING"' in r.output


def test_cli_unknown_agent(tmp_path):
    f = tmp_path / "q.sql"
    f.write_text("select 1")
    r = CliRunner().invoke(cli.main, ["review", str(f), "-a", "nope"])
    assert r.exit_code != 0 and "Unknown agent" in r.output
