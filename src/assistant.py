from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Any

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import ChatMessage, ChatMessageRole

from .config import Settings
from .query_service import QueryService, validate_read_only_sql


TABLE_REF_PATTERN = re.compile(r"\b([A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*)\b")


@dataclass
class AssistantAnswer:
    selected_table: str
    question: str
    generated_sql: str
    summary: str
    query_result: dict[str, Any]


def _extract_text_content(response: Any) -> str:
    if not getattr(response, "choices", None):
        return ""
    choice = response.choices[0]
    if hasattr(choice, "as_dict"):
        content = choice.as_dict().get("message", {}).get("content")
    else:
        content = choice.message.content
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts: list[str] = []
        for item in content:
            if isinstance(item, dict):
                if item.get("type") == "text":
                    parts.append(item.get("text", ""))
                    continue
                if item.get("text"):
                    parts.append(str(item.get("text")))
                    continue
            item_type = getattr(item, "type", None)
            item_text = getattr(item, "text", None)
            if item_type == "text" and item_text:
                parts.append(item_text)
        return "\n".join(part for part in parts if part)
    return str(content)


def _extract_json_block(text: str) -> dict[str, Any]:
    cleaned = text.strip()
    if cleaned.startswith("```"):
        cleaned = cleaned.strip("`")
        cleaned = cleaned.replace("json\n", "", 1).strip()
    start = text.find("{")
    end = text.rfind("}")
    if start == -1 or end == -1 or end <= start:
        raise ValueError(f"Could not parse JSON response from model: {text}")
    return json.loads(cleaned[cleaned.find("{") : cleaned.rfind("}") + 1])


class TableAssistant:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.query_service = QueryService(settings)
        self.workspace_client = self.query_service.client.workspace_client

    def list_tables(self) -> list[str]:
        result = self.query_service.run_query(f"SHOW TABLES IN {self.settings.catalog}.{self.settings.schema}")
        rows = result.get("rows") or []
        table_names: list[str] = []
        for row in rows:
            table_name = row.get("tableName") or row.get("tablename")
            if table_name:
                table_names.append(str(table_name))
        return sorted(table_names)

    def describe_table(self, table_name: str) -> list[dict[str, Any]]:
        full_name = self._qualify_table(table_name)
        result = self.query_service.run_query(f"DESCRIBE TABLE {full_name}")
        return result.get("rows") or []

    def answer_question(self, table_name: str, question: str) -> AssistantAnswer:
        full_table_name = self._qualify_table(table_name)
        schema_rows = self.describe_table(table_name)
        sql = self._generate_sql(full_table_name, schema_rows, question)
        validated_sql = self._validate_generated_sql(sql, full_table_name)
        query_result = self.query_service.run_query(validated_sql)
        summary = self._summarize_result(full_table_name, question, validated_sql, query_result)
        return AssistantAnswer(
            selected_table=full_table_name,
            question=question,
            generated_sql=validated_sql,
            summary=summary,
            query_result=query_result,
        )

    def _qualify_table(self, table_name: str) -> str:
        available = self.list_tables()
        simple_name = table_name.split(".")[-1]
        if simple_name not in available:
            raise ValueError(f"Table '{table_name}' is not available in {self.settings.catalog}.{self.settings.schema}")
        return f"{self.settings.catalog}.{self.settings.schema}.{simple_name}"

    def _generate_sql(self, full_table_name: str, schema_rows: list[dict[str, Any]], question: str) -> str:
        schema_text = self._schema_prompt_text(schema_rows)
        prompt = f"""
You are a data assistant that writes only safe read-only Databricks SQL.
Use only this table: {full_table_name}
Do not use joins.
Do not reference any other table.
Return exactly JSON with keys "sql" and "explanation".
The SQL must be a single SELECT statement.
Prefer LIMIT {self.settings.query_row_limit} unless the question clearly asks for an aggregate.

Table schema:
{schema_text}

User question:
{question}
""".strip()

        response = self.workspace_client.serving_endpoints.query(
            name=self.settings.chat_endpoint,
            messages=[ChatMessage(role=ChatMessageRole.USER, content=prompt)],
            max_tokens=600,
            temperature=0,
        )
        payload = _extract_json_block(_extract_text_content(response))
        sql = payload.get("sql", "").strip()
        if not sql:
            raise ValueError("Model did not return SQL.")
        return sql

    def _schema_prompt_text(self, schema_rows: list[dict[str, Any]]) -> str:
        cleaned_rows = [
            row for row in schema_rows if row.get("col_name") and not str(row.get("col_name")).startswith("#")
        ]
        priority_rows = [
            row
            for row in cleaned_rows
            if str(row.get("data_type", "")).lower() in {"timestamp", "date"}
            or "time" in str(row.get("col_name", "")).lower()
            or "date" in str(row.get("col_name", "")).lower()
        ]
        base_rows = cleaned_rows[:120]

        selected: list[dict[str, Any]] = []
        seen: set[str] = set()
        for row in priority_rows + base_rows:
            key = str(row.get("col_name"))
            if key not in seen:
                selected.append(row)
                seen.add(key)

        lines = [
            f"- {row.get('col_name')}: {row.get('data_type')}"
            for row in selected
        ]
        if len(cleaned_rows) > len(selected):
            lines.append(f"- ... plus {len(cleaned_rows) - len(selected)} additional columns not shown")
        return "\n".join(lines)

    def _validate_generated_sql(self, sql: str, full_table_name: str) -> str:
        validated_sql = validate_read_only_sql(sql)
        refs = {match.group(1).lower() for match in TABLE_REF_PATTERN.finditer(validated_sql)}
        if not refs:
            raise ValueError("Generated SQL did not reference a fully qualified table name.")
        if refs != {full_table_name.lower()}:
            raise ValueError(f"Generated SQL referenced unexpected tables: {sorted(refs)}")
        return validated_sql

    def _summarize_result(
        self,
        full_table_name: str,
        question: str,
        sql: str,
        query_result: dict[str, Any],
    ) -> str:
        compact_rows = (query_result.get("rows") or [])[:10]
        prompt = f"""
You are a concise analytics assistant.
Summarize the SQL results for the user in plain English.
Mention the selected table name.
If the result is empty, say so clearly.

Selected table: {full_table_name}
User question: {question}
SQL used: {sql}
Result rows: {json.dumps(compact_rows, default=str)}
""".strip()
        response = self.workspace_client.serving_endpoints.query(
            name=self.settings.chat_endpoint,
            messages=[ChatMessage(role=ChatMessageRole.USER, content=prompt)],
            max_tokens=300,
            temperature=0.1,
        )
        return _extract_text_content(response).strip()
