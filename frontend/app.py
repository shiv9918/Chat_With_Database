import json
import os
from pathlib import Path
from uuid import uuid4

import pandas as pd
import requests
import streamlit as st


API_URL = os.getenv("BACKEND_URL", "http://127.0.0.1:8000").rstrip("/")
HISTORY_FILE = Path(__file__).with_name(".query_history.json")
MAX_UPLOAD_MB = int(os.getenv("MAX_UPLOAD_MB", "20"))


def load_query_history() -> list[dict]:
    try:
        if HISTORY_FILE.exists():
            return json.loads(HISTORY_FILE.read_text(encoding="utf-8"))
    except Exception:
        return []
    return []


def save_query_history(history: list[dict]) -> None:
    try:
        HISTORY_FILE.write_text(json.dumps(history, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception:
        pass


def connect_backend(db_url: str) -> bool:
    """Connect to backend and cache schema for a given database URL."""
    try:
        res = requests.post(f"{API_URL}/connect", json={"db_url": db_url}, timeout=15)
        data = res.json()

        if res.status_code == 200 and data.get("success"):
            st.session_state.connected = True
            st.session_state.schema = data["schema"]
            st.session_state.db_type = data["db_type"]
            st.session_state.db_url = db_url
            st.session_state.last_query_response = None
            st.session_state.chat_messages = []
            st.session_state.chat_session_id = uuid4().hex
            return True

        st.error(f"❌ {data.get('detail', 'Connection failed')}")
        return False
    except requests.exceptions.ConnectionError:
        st.error(f"❌ Cannot reach backend at {API_URL}. Check FRONTEND env var BACKEND_URL.")
        return False
    except Exception as e:
        st.error(f"❌ Error: {str(e)}")
        return False


def connect_uploaded_file(uploaded_file) -> bool:
    """Upload CSV/Excel to backend and connect using backend-created temporary SQLite DB."""
    try:
        file_bytes = uploaded_file.getvalue()
        files = {
            "file": (
                uploaded_file.name,
                file_bytes,
                uploaded_file.type or "application/octet-stream",
            )
        }
        res = requests.post(f"{API_URL}/connect-upload", files=files, timeout=180)
        data = res.json()

        if res.status_code == 200 and data.get("success"):
            st.session_state.connected = True
            st.session_state.schema = data["schema"]
            st.session_state.db_type = data["db_type"]
            st.session_state.db_url = data["db_url"]
            st.session_state.last_query_response = None
            st.session_state.chat_messages = []
            st.session_state.chat_session_id = uuid4().hex
            return True

        st.error(f"❌ {data.get('detail', 'Upload connection failed')}")
        return False
    except requests.exceptions.ConnectionError:
        st.error(f"❌ Cannot reach backend at {API_URL}. Check FRONTEND env var BACKEND_URL.")
        return False
    except Exception as e:
        st.error(f"❌ Upload failed: {str(e)}")
        return False


st.set_page_config(page_title="Text-to-Query AI", page_icon="🧠", layout="wide")

st.title("🧠 Text-to-Query AI")
st.caption("Ask questions in English → Get real database results")
st.divider()

st.markdown(
    """
    <style>
    div[data-testid="stChatInput"] {
        max-width: 980px;
        margin: 0 auto;
    }
    div[data-testid="stChatInput"] textarea {
        border-radius: 28px;
    }
    </style>
    """,
    unsafe_allow_html=True,
)


if "connected" not in st.session_state:
    st.session_state.connected = False
if "schema" not in st.session_state:
    st.session_state.schema = {}
if "db_type" not in st.session_state:
    st.session_state.db_type = ""
if "db_url" not in st.session_state:
    st.session_state.db_url = ""
if "last_query_response" not in st.session_state:
    st.session_state.last_query_response = None
if "query_history" not in st.session_state:
    st.session_state.query_history = load_query_history()
if "chat_session_id" not in st.session_state:
    st.session_state.chat_session_id = uuid4().hex
if "chat_messages" not in st.session_state:
    st.session_state.chat_messages = []
if "composer_text" not in st.session_state:
    st.session_state.composer_text = ""


def run_query_request(question_text: str) -> dict | None:
    try:
        res = requests.post(
            f"{API_URL}/query",
            json={
                "db_url": st.session_state.db_url,
                "question": question_text,
                "session_id": st.session_state.chat_session_id,
            },
            timeout=30,
        )
        data = res.json()

        if res.status_code == 200 and data.get("success"):
            st.session_state.last_query_response = data
            history_item = {
                "id": uuid4().hex,
                "question": question_text,
                "db_type": data.get("db_type", st.session_state.db_type),
            }
            st.session_state.query_history = [history_item] + st.session_state.query_history[:9]
            save_query_history(st.session_state.query_history)
            return data

        st.session_state.last_query_response = None
        st.error(f"❌ {data.get('detail', 'Query failed')}")
        return None

    except requests.exceptions.ConnectionError:
        st.session_state.last_query_response = None
        st.error("❌ Cannot reach backend. Is it running?")
        return None
    except Exception as e:
        st.session_state.last_query_response = None
        st.error(f"❌ Unexpected error: {str(e)}")
        return None


def render_response_payload(payload: dict, key_prefix: str = "result") -> None:
    st.subheader("🔍 Query")

    query_items = payload.get("results") or [{
        "question": payload.get("question", ""),
        "query": payload.get("query", ""),
        "explanation": payload.get("explanation", ""),
        "repaired": payload.get("repaired", False),
        "result": payload.get("result", {"count": 0, "rows": []}),
    }]

    result_columns = st.columns(len(query_items)) if len(query_items) > 1 else [st.container()]

    for index, item in enumerate(query_items):
        result = item["result"]
        rows = result["rows"]
        container = result_columns[index] if len(query_items) > 1 else result_columns[0]

        with container:
            if item.get("query"):
                lang = "sql" if payload["db_type"] == "sql" else "javascript"
                st.code(item["query"], language=lang)

            if not rows:
                st.warning("Query ran successfully but returned no rows.")
                continue

            df = pd.DataFrame(rows)
            df_for_chart = df.copy()
            for col in df_for_chart.columns:
                if pd.api.types.is_object_dtype(df_for_chart[col]):
                    converted = pd.to_numeric(df_for_chart[col], errors="coerce")
                    if converted.notna().sum() > 0:
                        df_for_chart[col] = converted

            tab1, tab2, tab3 = st.tabs(["📋 Table", "📊 Bar Chart", "🥧 Pie Chart"])

            with tab1:
                st.dataframe(df, use_container_width=True, hide_index=True)
                st.download_button(
                    label="⬇️ Download as CSV",
                    data=df.to_csv(index=False),
                    file_name=f"query_results_{index + 1}.csv" if len(query_items) > 1 else "query_results.csv",
                    mime="text/csv",
                    key=f"{key_prefix}_download_{index}",
                )

            with tab2:
                numeric_cols = df_for_chart.select_dtypes(include="number").columns.tolist()
                text_cols = df_for_chart.select_dtypes(exclude="number").columns.tolist()

                if not numeric_cols and text_cols:
                    freq_col = st.selectbox(
                        "Category Column",
                        options=text_cols,
                        key=f"{key_prefix}_bar_cat_col_{index}",
                    )
                    top_n = st.slider(
                        "Top Categories",
                        min_value=3,
                        max_value=30,
                        value=10,
                        key=f"{key_prefix}_bar_cat_topn_{index}",
                    )
                    freq_df = (
                        df_for_chart[freq_col]
                        .fillna("(empty)")
                        .astype(str)
                        .value_counts()
                        .head(top_n)
                        .reset_index()
                    )
                    freq_df.columns = [freq_col, "count"]
                    st.bar_chart(freq_df.set_index(freq_col)[["count"]], use_container_width=True)
                    st.caption(f"Showing top {len(freq_df)} categories by frequency for **{freq_col}**")
                elif not numeric_cols:
                    st.info("📊 No chartable data found.")
                else:
                    col_a, col_b = st.columns(2)
                    with col_a:
                        x_axis = st.selectbox(
                            "X Axis (Label)",
                            options=text_cols if text_cols else df_for_chart.columns.tolist(),
                            key=f"{key_prefix}_bar_x_{index}",
                        )
                    with col_b:
                        y_axis = st.selectbox(
                            "Y Axis (Value)",
                            options=numeric_cols,
                            key=f"{key_prefix}_bar_y_{index}",
                        )

                    if x_axis == y_axis:
                        st.info("Pick different columns for X and Y to draw the bar chart.")
                    else:
                        chart_df = pd.DataFrame({x_axis: df_for_chart[x_axis], y_axis: df_for_chart[y_axis]}).set_index(x_axis)
                        st.bar_chart(chart_df[[y_axis]], use_container_width=True)
                        st.caption(f"Showing **{y_axis}** grouped by **{x_axis}**")

            with tab3:
                numeric_cols = df_for_chart.select_dtypes(include="number").columns.tolist()
                text_cols = df_for_chart.select_dtypes(exclude="number").columns.tolist()

                if not numeric_cols and text_cols:
                    pie_label = st.selectbox(
                        "Category Column",
                        options=text_cols,
                        key=f"{key_prefix}_pie_cat_label_{index}",
                    )
                    top_n = st.slider(
                        "Top Categories",
                        min_value=3,
                        max_value=30,
                        value=10,
                        key=f"{key_prefix}_pie_cat_topn_{index}",
                    )
                    freq_df = (
                        df_for_chart[pie_label]
                        .fillna("(empty)")
                        .astype(str)
                        .value_counts()
                        .head(top_n)
                        .reset_index()
                    )
                    freq_df.columns = [pie_label, "count"]

                    try:
                        import plotly.express as px

                        fig = px.pie(
                            freq_df,
                            names=pie_label,
                            values="count",
                            title=f"Top categories in {pie_label}",
                            hole=0.3,
                            color_discrete_sequence=px.colors.sequential.Plasma,
                        )
                        fig.update_traces(textposition="inside", textinfo="percent+label")
                        st.plotly_chart(fig, use_container_width=True)
                    except ImportError:
                        st.warning("Install plotly for pie charts: `pip install plotly`")
                        st.bar_chart(freq_df.set_index(pie_label)[["count"]], use_container_width=True)
                elif not numeric_cols:
                    st.info("🥧 No chartable data found.")
                else:
                    col_c, col_d = st.columns(2)
                    with col_c:
                        pie_label = st.selectbox(
                            "Label Column",
                            options=text_cols if text_cols else df_for_chart.columns.tolist(),
                            key=f"{key_prefix}_pie_label_{index}",
                        )
                    with col_d:
                        pie_value = st.selectbox(
                            "Value Column",
                            options=numeric_cols,
                            key=f"{key_prefix}_pie_value_{index}",
                        )

                    if pie_label == pie_value:
                        st.info("Pick different columns for Label and Value to draw the pie chart.")
                    else:
                        try:
                            import plotly.express as px

                            fig = px.pie(
                                df_for_chart,
                                names=pie_label,
                                values=pie_value,
                                title=f"{pie_value} by {pie_label}",
                                hole=0.3,
                                color_discrete_sequence=px.colors.sequential.Plasma,
                            )
                            fig.update_traces(textposition="inside", textinfo="percent+label")
                            st.plotly_chart(fig, use_container_width=True)
                        except ImportError:
                            st.warning("Install plotly for pie charts: `pip install plotly`")
                            st.bar_chart(df_for_chart[[pie_label, pie_value]].set_index(pie_label))


def send_chat_message(composer: str) -> None:
    composer = composer.strip()
    if not composer:
        st.warning("Please enter a message.")
        return

    user_message_id = uuid4().hex
    assistant_message_id = uuid4().hex
    st.session_state.chat_messages.append({"id": user_message_id, "role": "user", "content": composer})

    with st.spinner("🤖 AI is thinking and running the query..."):
        response = run_query_request(composer)

    if response:
        st.session_state.chat_messages.append({
            "id": assistant_message_id,
            "role": "assistant",
            "content": "",
            "payload": response,
        })


with st.sidebar:
    st.subheader("💬 Chats")

    if st.button("➕ New Chat", use_container_width=True):
        st.session_state.chat_messages = []
        st.session_state.chat_session_id = uuid4().hex
        st.session_state.composer_text = ""
        st.session_state.last_query_response = None
        st.rerun()

    st.divider()
    st.subheader("🔌 Database Connection")

    db_url = st.text_input(
        "Enter your DB URL",
        placeholder="sqlite:///test.db  or  mongodb+srv://...",
        help="Supports SQLite, PostgreSQL, MySQL, MongoDB",
    )

    with st.expander("📋 Supported URL Formats"):
        st.code("sqlite:///mydb.db", language="bash")
        st.code("postgresql://user:pass@host:5432/dbname", language="bash")
        st.code("postgresql+psycopg2://user:pass@host:5432/dbname", language="bash")
        st.code("postgres://user:pass@host:5432/dbname", language="bash")
        st.code("mysql://user:pass@host:3306/dbname", language="bash")
        st.code("mongodb://user:pass@host:27017/dbname", language="bash")
        st.code("mongodb+srv://user:pass@cluster/dbname", language="bash")

    if st.button("🔗 Connect to Database", use_container_width=True):
        if not db_url.strip():
            st.error("Please enter a DB URL first.")
        else:
            with st.spinner("Connecting and reading schema..."):
                if connect_backend(db_url):
                    st.success(f"✅ Connected! DB Type: `{st.session_state.db_type.upper()}`")

    st.divider()
    st.subheader("📄 CSV/Excel Upload")
    uploaded_file = st.file_uploader(
        "Upload CSV or Excel",
        type=["csv", "xlsx", "xls"],
        help="The file is loaded into a temporary SQLite table named uploaded_data.",
    )

    if st.button("⬆️ Use Uploaded File", use_container_width=True, disabled=uploaded_file is None):
        if uploaded_file is None:
            st.error("❌ Please upload a CSV/Excel file first.")
        else:
            file_size_mb = (uploaded_file.size or 0) / (1024 * 1024)
            if file_size_mb > MAX_UPLOAD_MB:
                st.error(
                    f"❌ File is too large ({file_size_mb:.1f} MB). "
                    f"Maximum allowed is {MAX_UPLOAD_MB} MB on this deployment."
                )
            else:
                with st.spinner("Uploading and preparing database..."):
                    if connect_uploaded_file(uploaded_file):
                        st.success("✅ Uploaded file connected as SQLite table `uploaded_data`.")

    if st.session_state.connected and st.session_state.schema:
        st.divider()
        st.subheader("📂 Detected Schema")
        st.caption(f"DB Type: `{st.session_state.db_type.upper()}`")

        for table, columns in st.session_state.schema.items():
            with st.expander(f"🗂️ {table} ({len(columns)} columns)"):
                for col in columns:
                    st.markdown(f"- `{col}`")

    st.divider()
    st.caption("Recent Questions")
    if st.session_state.query_history:
        for index, item in enumerate(st.session_state.query_history, start=1):
            label = f"↻ {item['question']}"
            if st.button(label, key=item.get("id", f"history_{index}"), use_container_width=True):
                st.session_state.composer_text = item["question"]
                st.rerun()
    else:
        st.caption("No queries yet. Your last 10 questions will appear here.")

st.subheader("💬 Chat With Your Database")

if not st.session_state.connected:
    st.info("👈 Connect to a database first (from the sidebar)")

for message in st.session_state.chat_messages:
    with st.chat_message(message["role"]):
        if message.get("content"):
            st.markdown(message.get("content", ""))
        if message["role"] == "assistant" and message.get("payload"):
            render_response_payload(message["payload"], key_prefix=message.get("id", "assistant"))

composer = st.chat_input(
    "Ask anything",
    key="composer_text",
    disabled=not st.session_state.connected,
)
if composer:
    send_chat_message(composer)
    st.rerun()

st.divider()
