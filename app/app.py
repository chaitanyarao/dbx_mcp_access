from __future__ import annotations

import pandas as pd
import streamlit as st

from assistant import AssistantAnswer, TableAssistant
from config import load_settings


st.set_page_config(
    page_title="Unity Catalog AI Assistant",
    page_icon=":material/table_chart:",
    layout="wide",
)


@st.cache_resource(show_spinner=False)
def get_assistant() -> TableAssistant:
    settings = load_settings()
    return TableAssistant(settings)


@st.cache_data(ttl=300, show_spinner=False)
def load_tables() -> list[str]:
    return get_assistant().list_tables()


@st.cache_data(ttl=300, show_spinner=False)
def load_schema(table_name: str) -> list[dict[str, str]]:
    return get_assistant().describe_table(table_name)


def _result_frame(answer: AssistantAnswer) -> pd.DataFrame | None:
    rows = answer.query_result.get("rows") or []
    if not rows:
        return None
    return pd.DataFrame(rows)


def _render_history() -> None:
    for item in st.session_state.messages:
        with st.chat_message(item["role"]):
            st.markdown(item["content"])
            if item.get("sql"):
                with st.expander("SQL used", expanded=False):
                    st.code(item["sql"], language="sql")
            if item.get("data") is not None:
                st.dataframe(item["data"], use_container_width=True, hide_index=True)


def main() -> None:
    st.title("Unity Catalog AI Assistant")
    st.caption("Select a Unity Catalog table, ask a question, and the app answers through the Databricks SQL MCP server.")

    if "messages" not in st.session_state:
        st.session_state.messages = []

    assistant = get_assistant()
    tables = load_tables()
    settings = assistant.settings

    with st.sidebar:
        st.subheader("Data Scope")
        st.write(f"Catalog: `{settings.catalog}`")
        st.write(f"Schema: `{settings.schema}`")
        if tables:
            selected_table = st.selectbox(
                "Table",
                options=tables,
                index=tables.index(settings.table) if settings.table in tables else 0,
            )
        else:
            selected_table = None
            st.selectbox("Table", options=[], index=None, placeholder="No options to select.")

        if st.button("Refresh table list", use_container_width=True):
            load_tables.clear()
            load_schema.clear()
            st.rerun()

        with st.expander("Selected table schema", expanded=False):
            if not selected_table:
                st.info("Choose a table after the app can see at least one table in the selected catalog and schema.")
            else:
                schema_rows = load_schema(selected_table)
                schema_frame = pd.DataFrame(schema_rows)
                if schema_frame.empty:
                    st.info("No schema details were returned.")
                else:
                    st.dataframe(schema_frame, use_container_width=True, hide_index=True)

        st.divider()
        st.markdown("Example questions")
        st.markdown("- How many rows are in this table?")
        st.markdown("- What timestamp columns are available?")
        st.markdown("- Show the latest 10 records.")
        st.markdown("- What are the top 5 values for a key status column?")

    if not tables:
        st.warning(
            "No tables were returned for this app in "
            f"`{settings.catalog}.{settings.schema}`. "
            "This usually means the app service principal does not yet have access to the SQL warehouse or the Unity Catalog schema."
        )
        st.info(
            "Grant the app access to the warehouse and table/schema, then refresh the table list. "
            f"App name: `{settings.app_name}`" if hasattr(settings, "app_name") else
            "Grant the app access to the warehouse and table/schema, then refresh the table list."
        )
        st.stop()

    _render_history()

    question = st.chat_input(f"Ask a question about {selected_table}")
    if not question:
        return

    st.session_state.messages.append({"role": "user", "content": question})
    with st.chat_message("user"):
        st.markdown(question)

    with st.chat_message("assistant"):
        with st.spinner("Running a read-only MCP-backed query..."):
            try:
                answer = assistant.answer_question(selected_table, question)
            except Exception as exc:
                error_text = f"I could not answer that question safely: {exc}"
                st.error(error_text)
                st.session_state.messages.append({"role": "assistant", "content": error_text})
                return

        st.markdown(answer.summary)
        with st.expander("SQL used", expanded=False):
            st.code(answer.generated_sql, language="sql")
        result_frame = _result_frame(answer)
        if result_frame is not None:
            st.dataframe(result_frame, use_container_width=True, hide_index=True)
        else:
            st.info("The query completed but returned no rows.")

    st.session_state.messages.append(
        {
            "role": "assistant",
            "content": answer.summary,
            "sql": answer.generated_sql,
            "data": result_frame,
        }
    )


if __name__ == "__main__":
    main()
