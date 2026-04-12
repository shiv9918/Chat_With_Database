import os
from pathlib import Path
from dotenv import load_dotenv
import json
import re

from langchain_groq import ChatGroq
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import StrOutputParser

# Load .env from backend first, then project root as fallback.
BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env", override=True)
load_dotenv(BASE_DIR.parent / ".env", override=True)

GROQ_API_KEY = (os.getenv("GROQ_API_KEY") or "").strip().strip('"').strip("'")
if not GROQ_API_KEY:
    raise RuntimeError(
        "Missing GROQ_API_KEY. Add it to backend/.env or project .env, "
        "or set it in your shell environment."
    )

# ─── Initialize Groq LLM via LangChain ───────────────────────────────────────

llm = ChatGroq(
    groq_api_key=GROQ_API_KEY,
    model="llama-3.3-70b-versatile",   # Best model on Groq for reasoning
    temperature=0,                      # 0 = deterministic, better for queries
    max_tokens=500,
)

output_parser = StrOutputParser()


# ─── Prompt Templates ────────────────────────────────────────────────────────

SQL_QUERY_PROMPT = ChatPromptTemplate.from_messages([
    ("system", """You are an expert SQL database engineer.
Your ONLY job is to convert English questions into valid SQL queries.

Rules:
- Return ONLY the raw SQL query
- No explanation, no markdown, no backticks, no comments
- Use JOINs when multiple tables are needed
- Use COUNT, SUM, AVG, GROUP BY when asked
- Never use column/table names not present in the schema
- Respect the SQL dialect provided
- For PostgreSQL, if identifiers can conflict with keywords (e.g., user, role), use double quotes for identifiers
- Always end with semicolon
"""),
    ("human", """SQL Dialect: {sql_dialect}

Database Schema:
{schema}

Question: {question}

SQL Query:""")
])


MONGO_QUERY_PROMPT = ChatPromptTemplate.from_messages([
    ("system", """You are an expert MongoDB engineer.
Your ONLY job is to convert English questions into valid PyMongo queries.

Rules:
- Return ONLY the raw PyMongo code
- No explanation, no markdown, no backticks, no comments
- Allowed formats only:
    1) db.collection.find(filter_dict)
    2) db.collection.find_one(filter_dict)
    3) db.collection.count_documents(filter_dict)
    4) db.collection.aggregate([...])
- Use $gt, $lt, $eq, $in, $regex, $and, $or for filters
- Use aggregate pipeline for grouping, sorting, counting
- Never reference fields not in the schema
"""),
    ("human", """Database Schema:
{schema}

Question: {question}

PyMongo Query:""")
])


EXPLANATION_PROMPT = ChatPromptTemplate.from_messages([
    ("system", "You explain database queries in simple English. 1-2 sentences max. No technical jargon."),
    ("human", """This {db_type} query was generated:
{query}

Explain what it does in plain English for a non-technical person:""")
])


REPAIR_QUERY_PROMPT = ChatPromptTemplate.from_messages([
    ("system", """You are an expert database query fixer.
Your ONLY job is to repair a broken database query using the schema, the user question, and the execution error.

Rules:
- Return ONLY the fixed raw query
- No explanation, no markdown, no backticks, no comments
- Preserve the original intent of the user's question
- Use only tables/collections and fields that exist in the schema
- If the database is PostgreSQL, quote reserved identifiers when needed
- For MongoDB, use only supported formats:
  1) db.collection.find(filter_dict)
  2) db.collection.find_one(filter_dict)
  3) db.collection.count_documents(filter_dict)
  4) db.collection.aggregate([...])
"""),
    ("human", """Database Type: {db_type}
SQL Dialect: {sql_dialect}

Database Schema:
{schema}

User Question:
{question}

Broken Query:
{query}

Execution Error:
{error}

Return the fixed query only:""")
])


REWRITE_FOLLOWUP_PROMPT = ChatPromptTemplate.from_messages([
    ("system", """You rewrite a follow-up user question into a standalone question.

Rules:
- Return ONLY the rewritten question
- No explanation, no markdown, no backticks, no comments
- Use the conversation history to resolve references like 'now', 'those', 'them', 'only', 'also', 'filter it'
- Keep the intent of the user's latest message
- If the latest message is already standalone, return it unchanged
"""),
    ("human", """Conversation History:
{chat_history}

Latest User Message:
{question}

Standalone Question:""")
])


# ─── LangChain Chains ────────────────────────────────────────────────────────

# Chain = Prompt | LLM | OutputParser
sql_chain      = SQL_QUERY_PROMPT   | llm | output_parser
mongo_chain    = MONGO_QUERY_PROMPT | llm | output_parser
explain_chain  = EXPLANATION_PROMPT | llm | output_parser
repair_chain    = REPAIR_QUERY_PROMPT | llm | output_parser
rewrite_chain   = REWRITE_FOLLOWUP_PROMPT | llm | output_parser


CHAT_MEMORIES: dict[str, list[tuple[str, str]]] = {}
MAX_CHAT_TURNS = 12


def get_chat_history_text(session_id: str) -> str:
    """Return formatted chat history text for follow-up rewriting."""
    turns = CHAT_MEMORIES.get(session_id, [])
    lines: list[str] = []
    for user_text, assistant_text in turns:
        lines.append(f"User: {user_text}")
        lines.append(f"Assistant: {assistant_text}")
    return "\n".join(lines)


def rewrite_followup_question(session_id: str, question: str) -> str:
    """Rewrite a follow-up question using in-process session history."""
    history = get_chat_history_text(session_id)

    if not history.strip():
        return question.strip()

    rewritten = rewrite_chain.invoke({
        "chat_history": history,
        "question": question,
    }).strip().strip("```").strip()

    return rewritten or question.strip()


def remember_chat_turn(session_id: str, user_question: str, assistant_text: str) -> None:
    """Store latest chat turn in memory with a fixed-size buffer."""
    turns = CHAT_MEMORIES.setdefault(session_id, [])
    turns.append((user_question, assistant_text))
    if len(turns) > MAX_CHAT_TURNS:
        del turns[:-MAX_CHAT_TURNS]


def _pick_mongo_collection(schema: dict, question: str) -> str:
    """Pick the best collection name from schema based on question text."""
    collections = list(schema.keys())
    if not collections:
        return "collection"

    q = question.lower()
    for name in collections:
        n = name.lower()
        if n in q or f"{n}s" in q or (n.endswith("s") and n[:-1] in q):
            return name

    return collections[0]


def _normalize_mongo_collection_name(raw_query: str, schema: dict, question: str) -> str:
    """Replace placeholder/wrong-case Mongo collection names with schema-valid names."""
    collections = list(schema.keys())
    if not collections:
        return raw_query

    preferred = _pick_mongo_collection(schema, question)

    pattern = r"^db\.(\w+)\.(find|aggregate|find_one|findOne|count|count_documents|estimated_document_count)\("
    match = re.match(pattern, raw_query)
    if not match:
        return raw_query

    generated_name = match.group(1)
    op = match.group(2)

    # If model used placeholders like 'collection' or 'collection_name', force best match.
    if generated_name.lower() in {"collection", "collection_name", "coll"}:
        return raw_query.replace(f"db.{generated_name}.{op}(", f"db.{preferred}.{op}(", 1)

    # If only case differs, map to exact schema casing.
    for actual in collections:
        if actual.lower() == generated_name.lower() and actual != generated_name:
            return raw_query.replace(f"db.{generated_name}.{op}(", f"db.{actual}.{op}(", 1)

    return raw_query


def _normalize_postgres_identifiers(raw_query: str, schema: dict) -> str:
    """Quote reserved PostgreSQL identifiers when they are known schema tables."""
    table_names = {name.lower(): name for name in schema.keys()}
    if "user" not in table_names:
        return raw_query

    # Quote table name in common table-position clauses.
    raw_query = re.sub(r"(?i)\bFROM\s+user\b", 'FROM "user"', raw_query)
    raw_query = re.sub(r"(?i)\bJOIN\s+user\b", 'JOIN "user"', raw_query)
    raw_query = re.sub(r"(?i)\bUPDATE\s+user\b", 'UPDATE "user"', raw_query)
    raw_query = re.sub(r"(?i)\bINTO\s+user\b", 'INTO "user"', raw_query)
    return raw_query


def _normalize_generated_query(db_type: str, schema: dict, question: str, raw_query: str, sql_dialect: str = "sql") -> str:
    """Apply lightweight deterministic cleanup to any generated query."""
    raw_query = raw_query.strip().strip("```").strip()

    if db_type == "mongodb":
        raw_query = raw_query.rstrip(";")
        raw_query = raw_query.replace(".findOne(", ".find_one(")
        raw_query = raw_query.replace(".countDocuments(", ".count_documents(")
        raw_query = raw_query.replace(".estimatedDocumentCount(", ".estimated_document_count(")
        raw_query = _normalize_mongo_collection_name(raw_query, schema, question)
    elif db_type == "sql" and sql_dialect == "postgresql":
        raw_query = _normalize_postgres_identifiers(raw_query, schema)

    return raw_query


# ─── Main Function ────────────────────────────────────────────────────────────

def generate_query(db_type: str, schema: dict, question: str, sql_dialect: str = "sql") -> dict:
    """
    Uses LangChain + Groq to generate a DB query from English.

    Returns:
    {
        "query": "SELECT ...",
        "explanation": "This fetches all users..."
    }
    """
    schema_str = json.dumps(schema, indent=2)

    # Step 1: Pick the right chain based on DB type
    if db_type == "sql":
        raw_query = sql_chain.invoke({
            "sql_dialect": sql_dialect,
            "schema": schema_str,
            "question": question
        })
    elif db_type == "mongodb":
        raw_query = mongo_chain.invoke({
            "schema": schema_str,
            "question": question
        })
    else:
        raise ValueError(f"Unsupported db_type: {db_type}")

    raw_query = _normalize_generated_query(db_type, schema, question, raw_query, sql_dialect=sql_dialect)

    # Step 2: Generate plain English explanation
    explanation = explain_chain.invoke({
        "db_type": db_type.upper(),
        "query": raw_query
    })

    return {
        "query": raw_query,
        "explanation": explanation.strip()
    }


def repair_query(db_type: str, schema: dict, question: str, broken_query: str, error_message: str, sql_dialect: str = "sql") -> dict:
    """Ask Groq to fix a broken query using the execution error."""
    schema_str = json.dumps(schema, indent=2)

    repaired_query = repair_chain.invoke({
        "db_type": db_type.upper(),
        "sql_dialect": sql_dialect,
        "schema": schema_str,
        "question": question,
        "query": broken_query,
        "error": error_message,
    })

    repaired_query = _normalize_generated_query(db_type, schema, question, repaired_query, sql_dialect=sql_dialect)

    explanation = explain_chain.invoke({
        "db_type": db_type.upper(),
        "query": repaired_query
    })

    return {
        "query": repaired_query,
        "explanation": explanation.strip()
    }


if __name__ == "__main__":
    test_schema = {
        "users": ["id", "name", "age", "email"],
        "orders": ["id", "user_id", "amount", "created_at"]
    }

    result = generate_query(
        db_type="sql",
        schema=test_schema,
        question="Show names of users who spent more than 1000"
    )

    print("Query:", result["query"])
    print("Explanation:", result["explanation"])