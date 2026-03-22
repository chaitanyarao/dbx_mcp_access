from __future__ import annotations

import json
import re
import time
from typing import Any

from config import Settings
from mcp_client import ManagedSqlMcpClient, ToolDetails


READ_ONLY_BLOCKLIST = (
    "insert",
    "update",
    "delete",
    "merge",
    "drop",
    "alter",
    "truncate",
    "create",
)
IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def validate_identifier(value: str, label: str) -> str:
    if not IDENTIFIER_PATTERN.match(value):
        raise ValueError(f"Invalid {label}: {value}")
    return value


def validate_read_only_sql(sql: str) -> str:
    normalized = sql.strip()
    lowered = normalized.lower()
    if not lowered.startswith(("select", "show", "describe")):
        raise ValueError("Only SELECT, SHOW, and DESCRIBE queries are allowed.")
    for keyword in READ_ONLY_BLOCKLIST:
        if re.search(rf"\b{keyword}\b", lowered):
            raise ValueError(f"Blocked non-read-only keyword detected: {keyword}")
    return normalized


def _content_to_text(content: Any) -> str:
    if content is None:
        return ""
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        chunks: list[str] = []
        for item in content:
            text = getattr(item, "text", None)
            if text:
                chunks.append(text)
                continue
            if isinstance(item, dict) and item.get("text"):
                chunks.append(str(item["text"]))
                continue
            chunks.append(json.dumps(item, default=str))
        return "\n".join(chunks)
    return json.dumps(content, default=str)


def response_to_text(response: Any) -> str:
    content = getattr(response, "content", None)
    if content is None and isinstance(response, dict):
        content = response.get("content")
    if content is not None:
        rendered = _content_to_text(content)
        if rendered:
            return rendered
    as_dict = getattr(response, "as_dict", None)
    if callable(as_dict):
        return json.dumps(as_dict(), indent=2, default=str)
    if isinstance(response, dict):
        return json.dumps(response, indent=2, default=str)
    return str(response)


def response_to_payload(response: Any) -> dict[str, Any]:
    raw_text = response_to_text(response)
    try:
        parsed = json.loads(raw_text)
    except json.JSONDecodeError:
        return {"raw_text": raw_text}

    result = {
        "statement_id": parsed.get("statement_id"),
        "status": (parsed.get("status") or {}).get("state"),
        "raw_response": parsed,
    }

    manifest = parsed.get("manifest") or {}
    schema = (manifest.get("schema") or {}).get("columns") or []
    rows = (((parsed.get("result") or {}).get("data_array")) or [])
    if schema and rows:
        column_names = [column["name"] for column in schema]
        result["columns"] = column_names
        result["rows"] = [
            {
                column_names[idx]: next(iter(value.values())) if value else None
                for idx, value in enumerate(row.get("values", []))
            }
            for row in rows
        ]
    result["row_count"] = manifest.get("total_row_count")
    result["truncated"] = manifest.get("truncated")
    return result


def choose_sql_tool(tools: list[ToolDetails]) -> ToolDetails:
    ranked = sorted(
        tools,
        key=lambda tool: (
            "read_only" not in tool.name.lower(),
            "execute" not in tool.name.lower(),
            "sql" not in f"{tool.name} {tool.description}".lower(),
            tool.name,
        ),
    )
    if not ranked:
        raise RuntimeError("No MCP tools were returned by the server.")
    return ranked[0]


def infer_sql_arguments(tool: ToolDetails, sql: str, settings: Settings) -> dict[str, Any]:
    properties = (tool.input_schema or {}).get("properties", {})
    arguments: dict[str, Any] = {}

    for key in properties:
        lowered = key.lower()
        if lowered in {"statement", "sql", "query"}:
            arguments[key] = sql
        elif lowered in {"warehouse_id", "warehouseid"}:
            arguments[key] = settings.sql_warehouse_id
        elif lowered in {"timeout_seconds", "timeout", "wait_timeout_seconds"}:
            arguments[key] = settings.query_timeout_seconds
        elif lowered in {"row_limit", "limit", "max_rows"}:
            arguments[key] = settings.query_row_limit
        elif lowered == "catalog":
            arguments[key] = settings.catalog
        elif lowered == "schema":
            arguments[key] = settings.schema

    if not any(name in arguments for name in properties if name.lower() in {"statement", "sql", "query"}):
        arguments["statement"] = sql
    if properties and not any(name.lower() in {"statement", "sql", "query"} for name in properties):
        arguments["query"] = sql
        arguments.pop("statement", None)
    if properties and not any(name in arguments for name in properties if name.lower() in {"warehouse_id", "warehouseid"}):
        arguments.pop("warehouse_id", None)
    elif not properties:
        arguments["warehouse_id"] = settings.sql_warehouse_id

    return arguments


class QueryService:
    def __init__(self, settings: Settings):
        self.settings = settings
        validate_identifier(settings.catalog, "catalog")
        validate_identifier(settings.schema, "schema")
        validate_identifier(settings.table, "table")
        self.client = ManagedSqlMcpClient(settings)

    def list_tools(self) -> list[ToolDetails]:
        return self.client.list_tools()

    def _poll_if_needed(self, payload: dict[str, Any], tools: list[ToolDetails]) -> dict[str, Any]:
        status = (payload.get("status") or "").upper()
        statement_id = payload.get("statement_id")
        if status in {"SUCCEEDED", "FAILED", "CANCELED"} or not statement_id:
            return payload

        poll_tool = next((tool for tool in tools if tool.name == "poll_sql_result"), None)
        if poll_tool is None:
            return payload

        deadline = time.time() + self.settings.query_timeout_seconds
        while time.time() < deadline:
            response = self.client.call_tool(poll_tool.name, {"statement_id": statement_id})
            payload = response_to_payload(response)
            status = (payload.get("status") or "").upper()
            if status in {"SUCCEEDED", "FAILED", "CANCELED"}:
                return payload
            time.sleep(1)

        raise TimeoutError(f"Query did not complete within {self.settings.query_timeout_seconds} seconds.")

    def run_query(self, sql: str) -> dict[str, Any]:
        validated_sql = validate_read_only_sql(sql)
        tools = self.list_tools()
        sql_tool = choose_sql_tool(tools)
        arguments = infer_sql_arguments(sql_tool, validated_sql, self.settings)
        response = self.client.call_tool(sql_tool.name, arguments)
        payload = self._poll_if_needed(response_to_payload(response), tools)
        return {
            "tool_name": sql_tool.name,
            "arguments": arguments,
            "sql": validated_sql,
            "statement_id": payload.get("statement_id"),
            "status": payload.get("status"),
            "row_count": payload.get("row_count"),
            "truncated": payload.get("truncated"),
            "columns": payload.get("columns"),
            "rows": payload.get("rows"),
            "raw_response": payload.get("raw_response"),
        }
