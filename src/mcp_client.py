from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from databricks.sdk import WorkspaceClient
from databricks_mcp import DatabricksMCPClient

from .config import Settings


@dataclass(frozen=True)
class ToolDetails:
    name: str
    description: str
    input_schema: dict[str, Any]


def build_workspace_client(settings: Settings) -> WorkspaceClient:
    if settings.auth_mode == "cli":
        return WorkspaceClient(profile=settings.profile)
    return WorkspaceClient()


class ManagedSqlMcpClient:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.workspace_client = build_workspace_client(settings)
        self.client = DatabricksMCPClient(
            server_url=settings.mcp_server_url,
            workspace_client=self.workspace_client,
        )

    def list_tools(self) -> list[ToolDetails]:
        return [
            ToolDetails(
                name=tool.name,
                description=tool.description or "",
                input_schema=getattr(tool, "inputSchema", {}) or {},
            )
            for tool in self.client.list_tools()
        ]

    def call_tool(self, tool_name: str, arguments: dict[str, Any]) -> Any:
        return self.client.call_tool(tool_name, arguments)
