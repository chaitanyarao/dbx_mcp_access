from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _load_key_value_file(path: Path) -> None:
    if not path.exists():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line or line.lstrip().startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip())


def load_environment() -> None:
    dot_env = ROOT / ".env"
    if dot_env.exists():
        _load_key_value_file(dot_env)

    env_file = os.getenv("DATABRICKS_ENV_FILE", "").strip()
    if env_file:
        _load_key_value_file((ROOT / env_file).resolve())


@dataclass(frozen=True)
class Settings:
    auth_mode: str
    profile: str
    host: str
    mcp_server_url: str
    sql_warehouse_id: str
    chat_endpoint: str
    catalog: str
    schema: str
    table: str
    query_row_limit: int
    query_timeout_seconds: int
    output_format: str

    @property
    def full_table_name(self) -> str:
        return f"{self.catalog}.{self.schema}.{self.table}"


def _required(name: str, default: str | None = None) -> str:
    value = os.getenv(name, default if default is not None else "")
    value = value.strip()
    if not value:
        raise ValueError(f"Missing required setting: {name}")
    return value


def load_settings() -> Settings:
    load_environment()
    host = _required("DATABRICKS_HOST").rstrip("/")
    mcp_server_url = os.getenv("DATABRICKS_MCP_SERVER_URL", f"{host}/api/2.0/mcp/sql").strip()
    return Settings(
        auth_mode=os.getenv("DATABRICKS_AUTH_MODE", "cli").strip().lower(),
        profile=os.getenv("DATABRICKS_PROFILE", "DEFAULT").strip(),
        host=host,
        mcp_server_url=mcp_server_url,
        sql_warehouse_id=_required("DATABRICKS_SQL_WAREHOUSE_ID"),
        chat_endpoint=_required("DATABRICKS_CHAT_ENDPOINT", "databricks-gpt-oss-20b"),
        catalog=_required("DATABRICKS_CATALOG"),
        schema=_required("DATABRICKS_SCHEMA"),
        table=_required("DATABRICKS_TABLE"),
        query_row_limit=int(os.getenv("QUERY_ROW_LIMIT", "100")),
        query_timeout_seconds=int(os.getenv("QUERY_TIMEOUT_SECONDS", "60")),
        output_format=os.getenv("OUTPUT_FORMAT", "text").strip().lower(),
    )
