from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from schema_extractor import extract_schema
from query_generator import (
    generate_query,
    repair_query,
    rewrite_followup_question,
    remember_chat_turn,
)
from query_executor import execute_query
from db_manager import detect_db_type, detect_sql_dialect
import os
import re

app = FastAPI(title="Text-to-Query API", version="1.0")

default_origins = [
    "http://localhost:5173",
    "http://localhost:8501",
]
cors_origins_raw = os.getenv("CORS_ORIGINS", "")
cors_origins = [origin.strip() for origin in cors_origins_raw.split(",") if origin.strip()] or default_origins

app.add_middleware(
    CORSMiddleware,
    allow_origins=cors_origins,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ─── Request Models ─────────────────────────────────────────────────────────

class ConnectRequest(BaseModel):
    db_url: str

class QueryRequest(BaseModel):
    db_url: str
    question: str
    session_id: str | None = None


def run_single_query(db_url: str, db_type: str, schema: dict, question: str, sql_dialect: str = "") -> dict:
    """Generate, execute, and self-heal a single query."""
    ai_result = generate_query(db_type, schema, question, sql_dialect=sql_dialect)
    query = ai_result["query"]
    explanation = ai_result["explanation"]
    repaired = False

    try:
        data = execute_query(db_url, query)
    except Exception as exec_error:
        repaired_result = repair_query(
            db_type=db_type,
            schema=schema,
            question=question,
            broken_query=query,
            error_message=str(exec_error),
            sql_dialect=sql_dialect,
        )
        query = repaired_result["query"]
        explanation = repaired_result["explanation"]
        repaired = True
        data = execute_query(db_url, query)

    return {
        "question": question,
        "resolved_question": question,
        "query": query,
        "explanation": explanation,
        "repaired": repaired,
        "result": data,
    }


# ─── Validation ──────────────────────────────────────────────────────────────

ALLOWED_PREFIXES = [
    "mongodb://",
    "mongodb+srv://",
    "postgresql://",
    "postgresql+psycopg2://",
    "postgres://",
    "mysql://",
    "sqlite:///",
]

def validate_db_url(db_url: str):
    if not any(db_url.startswith(prefix) for prefix in ALLOWED_PREFIXES):
        raise HTTPException(
            status_code=400,
            detail=f"Invalid DB URL. Allowed: {', '.join(ALLOWED_PREFIXES)}"
        )


# ─── Routes ──────────────────────────────────────────────────────────────────

@app.get("/")
def root():
    return {"message": "Text-to-Query API is running 🚀"}


@app.post("/connect")
def connect_and_get_schema(req: ConnectRequest):
    """
    Step 1: User provides DB URL
    Returns: DB type + full schema
    """
    validate_db_url(req.db_url)
    try:
        result = extract_schema(req.db_url)
        return {
            "success": True,
            "db_type": result["db_type"],
            "schema": result["schema"]
        }
    except ConnectionError as e:
        raise HTTPException(status_code=503, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/query")
def run_natural_language_query(req: QueryRequest):
    """
    Main endpoint — Full pipeline:
    question → AI query → execute → return results
    """
    validate_db_url(req.db_url)
    
    if not req.question.strip():
        raise HTTPException(status_code=400, detail="Question cannot be empty.")
    
    try:
        # 1. Extract schema
        schema_result = extract_schema(req.db_url)
        db_type = schema_result["db_type"]
        schema = schema_result["schema"]
        sql_dialect = detect_sql_dialect(req.db_url) if db_type == "sql" else ""
        session_id = req.session_id or "default-session"

        resolved_question = rewrite_followup_question(session_id, req.question)

        # 2. Run exactly one query per user request.
        primary_result = run_single_query(
            req.db_url,
            db_type,
            schema,
            resolved_question,
            sql_dialect=sql_dialect,
        )
        remember_chat_turn(
            session_id,
            req.question,
            f"{primary_result['explanation']} | Query: {primary_result['query']} | Rows: {primary_result['result'].get('count', 0)}",
        )
        
        return {
            "success": True,
            "db_type": db_type,
            "question": req.question,
            "resolved_question": resolved_question,
            "query": primary_result["query"],
            "explanation": primary_result["explanation"],
            "repaired": primary_result["repaired"],
            "result": primary_result["result"],
            "multi_query": False,
            "results": [primary_result],
        }
    
    except PermissionError as e:
        raise HTTPException(status_code=403, detail=str(e))
    except ConnectionError as e:
        raise HTTPException(status_code=503, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ─── Run ─────────────────────────────────────────────────────────────────────
# uvicorn main:app --reload --port 8000