# Databricks MCP Access

This project includes:

- a CLI for Databricks SQL MCP connectivity and read-only query execution
- an AI assistant that answers table questions using a Databricks chat model plus MCP-backed SQL execution
- a Databricks-hosted Streamlit app that lists Unity Catalog tables, lets the user select one, and answers questions about the selected table

## Commands

```powershell
python -m src.main test-connection
python -m src.main list-tools
python -m src.main list-tables
python -m src.main preview-table --limit 5
python -m src.main count-rows
python -m src.main run-query --sql "SELECT COUNT(*) FROM cmidev.telematics_primary.telematics_master_hb_e"
python -m src.main describe-table --table telematics_master_hb_e
python -m src.main ask --table telematics_master_hb_e --question "How many rows are in this table?"
python -m src.main chat
```

## Streamlit App

The Databricks app lives under [`app/`](./app) and uses workspace-native auth when hosted in Databricks.

Local run:

```powershell
cd app
streamlit run app.py
```

Bundle deploy:

```powershell
databricks bundle validate
databricks bundle deploy
databricks apps deploy unity-catalog-mcp-assistant --source-code-path "/Workspace/Users/kt351@cummins.com/.bundle/dbx-mcp-assistant/dev/files/app"
```

## Configuration

Copy `.env.example` to `.env` and update as needed. For this workspace, the easiest option is:

- `DATABRICKS_AUTH_MODE=env`
- `DATABRICKS_ENV_FILE=../Data Agent/.databricks/.databricks.env`
- `DATABRICKS_CHAT_ENDPOINT=databricks-gpt-oss-20b`

You can also use CLI auth by setting:

- `DATABRICKS_AUTH_MODE=cli`
- `DATABRICKS_PROFILE=<your-profile>`
