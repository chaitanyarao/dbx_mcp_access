# Plan for Building a Program to Access Databricks MCP and Query a Table

## 1) Goal

Build a small program that connects to a Databricks MCP server, accesses a target table, and executes read-only queries against that table.

Primary outcomes:

- Connect successfully to Databricks MCP from a local program.
- Authenticate securely using a supported Databricks auth flow.
- Access one or more approved Unity Catalog tables.
- Execute safe read-only queries and return results in a usable format.
- Provide a simple path for future expansion into natural-language or agent-based querying.

## 2) Recommended MCP approach

Use the Databricks SQL managed MCP server for direct table querying.

Recommended server:

- `https://adb-7018564609060221.1.azuredatabricks.net/api/2.0/mcp/sql`

Alternative server:

- If the goal shifts from direct SQL to natural-language business questions over curated resources, use the Genie MCP server:
  - `https://<workspace-hostname>/api/2.0/mcp/genie/{genie_space_id}`

Recommended choice for this project:

- `DBSQL MCP`

Reason:

- It is the better fit for a program that must access a known table and execute deterministic read-only SQL queries.

## 3) Scope

In scope:

- Connect to Databricks MCP
- Authenticate to the Databricks workspace
- Access an approved table in Unity Catalog
- Run read-only SQL queries
- Return results as console output and optionally JSON
- Add query safety checks and validation

Out of scope for phase 1:

- Write-back operations
- Table creation or schema changes
- Broad agent orchestration
- Full production deployment pipeline
- Natural-language to SQL unless added in a later phase

## 4) Architecture

Suggested program structure:

- `src/config.py`
  - Load workspace host, auth mode, MCP URL, default catalog/schema/table, and limits
- `src/mcp_client.py`
  - Create and manage the MCP connection
- `src/query_service.py`
  - Validate table names and execute safe read-only queries
- `src/main.py`
  - CLI entry point for preview and query commands
- `.env.example`
  - Environment variable template
- `requirements.txt`
  - Python dependencies

## 5) Authentication plan

Recommended auth for local development:

- Databricks CLI auth

Recommended auth for automation or production:

- OAuth app credentials

Phase 1 default:

- Use local Databricks CLI auth because it is the fastest path to a working prototype.

## 6) Configuration details to capture

The program should support the following config values:

- `DATABRICKS_HOST`
- `DATABRICKS_MCP_SERVER_URL`
- `DATABRICKS_AUTH_MODE`
- `DATABRICKS_SQL_WAREHOUSE_ID`
- `DATABRICKS_CATALOG`
- `DATABRICKS_SCHEMA`
- `DATABRICKS_TABLE`
- `QUERY_ROW_LIMIT`
- `QUERY_TIMEOUT_SECONDS`

Initial expected defaults:

- `DATABRICKS_AUTH_MODE=cli`
- `QUERY_ROW_LIMIT=100`
- `QUERY_TIMEOUT_SECONDS=60`

Inferred defaults for this workspace:

- `DATABRICKS_HOST=https://adb-7018564609060221.1.azuredatabricks.net`
- `DATABRICKS_MCP_SERVER_URL=https://adb-7018564609060221.1.azuredatabricks.net/api/2.0/mcp/sql`
- `DATABRICKS_SQL_WAREHOUSE_ID=767b7290bc20efd6`
- `DATABRICKS_CATALOG=cmidev`
- `DATABRICKS_SCHEMA=telematics_primary`
- `DATABRICKS_TABLE=telematics_master_hb_e`

## 7) Query safety rules

The program must be read-only.

Disallow:

- `INSERT`
- `UPDATE`
- `DELETE`
- `MERGE`
- `DROP`
- `ALTER`
- `TRUNCATE`
- `CREATE`

Additional safeguards:

- Require fully qualified table names or validated default catalog/schema settings
- Enforce a maximum row limit
- Apply timeouts
- Log only non-sensitive metadata

## 8) Program capabilities

Phase 1 commands:

- `test-connection`
  - Connect to MCP and verify authentication
- `preview-table`
  - Run `SELECT * FROM <table> LIMIT <n>`
- `count-rows`
  - Run `SELECT COUNT(*) FROM <table>`
- `run-query`
  - Execute an allowed read-only SQL statement

Optional phase 1 output formats:

- text table
- JSON

## 9) Example queries

Preview:

```sql
SELECT *
FROM cmidev.telematics_primary.telematics_master_hb_e
LIMIT 10;
```

Count:

```sql
SELECT COUNT(*)
FROM cmidev.telematics_primary.telematics_master_hb_e;
```

Recent grouped summary:

```sql
SELECT occurrence_date_time, COUNT(*) AS row_count
FROM cmidev.telematics_primary.telematics_master_hb_e
GROUP BY occurrence_date_time
ORDER BY occurrence_date_time DESC
LIMIT 30;
```

Observed schema note:

- The heartbeat table includes `occurrence_date_time`, which is a better initial timestamp column for example queries than the earlier `event_ts` placeholder.

If this program is used for telematics data later, add domain-specific queries for:

- heartbeat volume trends
- fault-code frequency
- VIN or ESN filtered slices
- daily or hourly rollups

## 10) Implementation phases

### Phase 1: Connectivity

- Set up Python project structure
- Configure auth and environment variables
- Connect to the Databricks SQL MCP server
- Verify the connection by listing available tools or successfully running a simple query

### Phase 2: Safe query execution

- Add read-only SQL validation
- Add table name validation
- Implement preview, count, and ad hoc query execution
- Add row limits and timeouts

### Phase 3: Usability

- Add CLI commands and helpful output formatting
- Add JSON output option
- Improve error messages for auth, permissions, and invalid SQL

### Phase 4: Hardening

- Add unit tests
- Add integration tests
- Add OAuth option for non-local usage
- Add allowlist controls for approved tables

## 11) Testing plan

Unit tests:

- config loading
- query validation
- table identifier validation
- limit enforcement

Integration tests:

- connect to MCP successfully
- preview target table successfully
- count rows successfully
- run one grouped summary query successfully

## 12) Success criteria

The project is successful when:

- The program authenticates to Databricks MCP without manual workaround steps
- The target table can be previewed
- At least three read-only queries run successfully
- Unsafe SQL is rejected
- Results are returned in a clean, repeatable format

## 13) Open configuration items

Inferred values for the initial implementation:

- `workspace_host`: `adb-7018564609060221.1.azuredatabricks.net`
- `mcp_server_url`: `https://adb-7018564609060221.1.azuredatabricks.net/api/2.0/mcp/sql`
- `sql_warehouse_id`: `767b7290bc20efd6`
- `catalog`: `cmidev`
- `schema`: `telematics_primary`
- `table`: `telematics_master_hb_e`
- `date_column_for_examples`: `occurrence_date_time`
- `preferred_output_format`: `text`

Items to verify once implementation starts:

- Confirm whether the first target table should stay on heartbeat data or switch to `cmidev.telematics_primary.telematics_master_fc_e`

## 14) Recommended next step

Implement a Python CLI prototype that uses Databricks SQL MCP with local CLI auth, targets one approved table, and supports:

- `test-connection`
- `preview-table`
- `count-rows`
- `run-query`
