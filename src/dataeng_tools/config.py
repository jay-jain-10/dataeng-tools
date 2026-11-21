"""Config loading: ~/.dataeng/config.yaml, overridable by env vars."""
import os
import warnings
from dataclasses import dataclass
from pathlib import Path

import yaml

def config_path() -> Path:
    return Path(os.environ.get("DATAENG_CONFIG") or Path.home() / ".dataeng" / "config.yaml")
DEFAULT_MODEL = "claude-sonnet-5-5"

TEMPLATE = """\
# dataeng-tools config
# anthropic_api_key: sk-ant-...   # or set ANTHROPIC_API_KEY (preferred)
model: claude-sonnet-5-5
warehouse: null        # snowflake | bigquery | clickhouse | redshift  (hint for agents)
orchestrator: null     # airflow | dagster
cloud: null            # aws | gcp | azure
max_tokens: 4096
"""


class ConfigError(Exception):
    pass


@dataclass
class Config:
    api_key: str | None = None
    model: str = DEFAULT_MODEL
    warehouse: str | None = None
    orchestrator: str | None = None
    cloud: str | None = None
    max_tokens: int = 4096

    def context_hint(self) -> str:
        parts = [f"{k}: {v}" for k, v in
                 [("warehouse", self.warehouse), ("orchestrator", self.orchestrator), ("cloud", self.cloud)] if v]
        return ("User's stack hints -> " + ", ".join(parts)) if parts else ""


def load_config(path: Path | None = None) -> Config:
    path = path or config_path()
    data = {}
    if path.exists():
        try:
            data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        except yaml.YAMLError as e:
            raise ConfigError(f"Invalid YAML in {path}: {e}")
        if not isinstance(data, dict):
            raise ConfigError(f"{path} must contain a YAML mapping (key: value).")
        if data.get("anthropic_api_key") and path.stat().st_mode & 0o077:
            warnings.warn(f"{path} holds an API key but is readable by others; run: chmod 600 {path}")
    try:
        max_tokens = int(data.get("max_tokens") or 4096)
    except (TypeError, ValueError):
        raise ConfigError("max_tokens in config must be an integer.")
    return Config(
        api_key=os.environ.get("ANTHROPIC_API_KEY") or data.get("anthropic_api_key"),
        model=os.environ.get("DATAENG_MODEL") or data.get("model") or DEFAULT_MODEL,
        warehouse=data.get("warehouse"),
        orchestrator=data.get("orchestrator"),
        cloud=data.get("cloud"),
        max_tokens=max_tokens,
    )


def init_config(path: Path | None = None) -> Path:
    path = path or config_path()
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    if not path.exists():
        path.write_text(TEMPLATE)
        path.chmod(0o600)
    return path
