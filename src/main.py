from __future__ import annotations

import argparse
import json
import sys

from .assistant import TableAssistant
from .config import load_settings
from .query_service import QueryService


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="CLI for Databricks SQL MCP access.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("test-connection", help="Validate MCP connectivity and tool discovery.")
    subparsers.add_parser("list-tools", help="List tools exposed by the configured MCP server.")
    subparsers.add_parser("list-tables", help="List available tables in the configured catalog and schema.")

    preview = subparsers.add_parser("preview-table", help="Preview rows from the configured table.")
    preview.add_argument("--limit", type=int, default=None, help="Optional preview row limit.")

    subparsers.add_parser("count-rows", help="Count rows in the configured table.")

    query = subparsers.add_parser("run-query", help="Run a read-only SELECT query.")
    query.add_argument("--sql", required=True, help="Read-only SQL statement to execute.")

    describe = subparsers.add_parser("describe-table", help="Describe a table in the configured schema.")
    describe.add_argument("--table", required=True, help="Table name to describe.")

    ask = subparsers.add_parser("ask", help="Ask a question about a selected table.")
    ask.add_argument("--table", required=True, help="Table name to query.")
    ask.add_argument("--question", required=True, help="Natural-language question about the table.")

    subparsers.add_parser("chat", help="Interactive assistant with table selection.")
    return parser


def emit(payload: object, output_format: str) -> None:
    if output_format == "json":
        print(json.dumps(payload, indent=2, default=str))
        return

    if isinstance(payload, list):
        for item in payload:
            print(f"- {item}")
        return

    if isinstance(payload, dict):
        if {"selected_table", "question", "generated_sql", "summary", "query_result"}.issubset(payload.keys()):
            print(f"selected_table: {payload.get('selected_table')}")
            print(f"question: {payload.get('question')}")
            print(f"generated_sql: {payload.get('generated_sql')}")
            print("summary:")
            print(payload.get("summary"))
            print("query_result:")
            print(json.dumps(payload.get("query_result"), indent=2, default=str))
            return
        if {"tool_name", "sql", "status"}.issubset(payload.keys()):
            print(f"tool_name: {payload.get('tool_name')}")
            print(f"sql: {payload.get('sql')}")
            print(f"status: {payload.get('status')}")
            if payload.get("statement_id"):
                print(f"statement_id: {payload.get('statement_id')}")
            if payload.get("row_count") is not None:
                print(f"row_count: {payload.get('row_count')}")
            if payload.get("truncated") is not None:
                print(f"truncated: {payload.get('truncated')}")
            if payload.get("columns"):
                print("columns:")
                print(json.dumps(payload.get("columns"), indent=2, default=str))
            if payload.get("rows") is not None:
                print("rows:")
                print(json.dumps(payload.get("rows"), indent=2, default=str))
            return
        for key, value in payload.items():
            if isinstance(value, (dict, list)):
                print(f"{key}:")
                print(json.dumps(value, indent=2, default=str))
            else:
                print(f"{key}: {value}")
        return

    print(payload)


def main() -> int:
    args = build_parser().parse_args()
    settings = load_settings()
    service = QueryService(settings)
    assistant = TableAssistant(settings)

    if args.command == "test-connection":
        emit(service.test_connection(), settings.output_format)
        return 0
    if args.command == "list-tools":
        tools = [
            {
                "name": tool.name,
                "description": tool.description,
                "input_schema": tool.input_schema,
            }
            for tool in service.list_tools()
        ]
        emit(tools, settings.output_format)
        return 0
    if args.command == "list-tables":
        emit(assistant.list_tables(), settings.output_format)
        return 0
    if args.command == "preview-table":
        emit(service.preview_table(args.limit), settings.output_format)
        return 0
    if args.command == "count-rows":
        emit(service.count_rows(), settings.output_format)
        return 0
    if args.command == "describe-table":
        emit(assistant.describe_table(args.table), settings.output_format)
        return 0
    if args.command == "run-query":
        emit(service.run_query(args.sql), settings.output_format)
        return 0
    if args.command == "ask":
        answer = assistant.answer_question(args.table, args.question)
        emit(answer.__dict__, settings.output_format)
        return 0
    if args.command == "chat":
        tables = assistant.list_tables()
        print("Available tables:")
        for idx, table in enumerate(tables, start=1):
            print(f"{idx}. {table}")
        choice = input("Select a table by number or name: ").strip()
        selected = choice
        if choice.isdigit():
            selected = tables[int(choice) - 1]
        print(f"Using table: {selected}")
        while True:
            question = input("Ask a question (or type exit): ").strip()
            if question.lower() in {"exit", "quit"}:
                break
            answer = assistant.answer_question(selected, question)
            emit(answer.__dict__, settings.output_format)
        return 0

    raise ValueError(f"Unsupported command: {args.command}")


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        raise SystemExit(1)
