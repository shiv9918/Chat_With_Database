from sqlalchemy import create_engine
from pymongo import MongoClient
import certifi
import pymongo.ssl_support as pymongo_ssl_support
from urllib.parse import urlparse


def _is_unreachable_cloud_host(hostname: str | None) -> bool:
    if not hostname:
        return False
    normalized = hostname.lower()
    return normalized in {"localhost", "127.0.0.1", "0.0.0.0", "::1"}

# PyMongo can prefer a PyOpenSSL-backed TLS path when pyOpenSSL is installed.
# In this environment that path is incompatible with the installed crypto stack,
# so force PyMongo to use the stdlib ssl implementation instead.
pymongo_ssl_support.HAVE_PYSSL = False

def detect_db_type(db_url: str) -> str:
    """Returns 'sql' or 'mongodb' based on URL"""
    if db_url.startswith("mongodb://") or db_url.startswith("mongodb+srv://"):
        return "mongodb"
    elif (
        db_url.startswith("postgresql://")
        or db_url.startswith("postgresql+psycopg2://")
        or db_url.startswith("postgres://")
        or db_url.startswith("mysql://")
        or db_url.startswith("sqlite:///")
    ):
        return "sql"
    else:
        raise ValueError(f"Unsupported DB URL format: {db_url[:20]}...")


def detect_sql_dialect(db_url: str) -> str:
    """Return concrete SQL dialect name for prompt guidance."""
    if db_url.startswith("postgresql") or db_url.startswith("postgres://"):
        return "postgresql"
    if db_url.startswith("mysql"):
        return "mysql"
    if db_url.startswith("sqlite"):
        return "sqlite"
    return "sql"


def get_sql_engine(db_url: str):
    """Returns a SQLAlchemy engine"""
    try:
        parsed = urlparse(db_url)
        if _is_unreachable_cloud_host(parsed.hostname):
            raise ConnectionError(
                "SQL connection failed: The host in your DB URL is localhost. "
                "From Render, localhost points to the Render container itself, not your local machine. "
                "Use a publicly reachable database host (for example Render Postgres/Supabase/Neon)."
            )

        # Use pg8000 for PostgreSQL to avoid psycopg2 dependency issues on cloud runtimes.
        if db_url.startswith("postgres://"):
            db_url = db_url.replace("postgres://", "postgresql+pg8000://", 1)
        elif db_url.startswith("postgresql+psycopg2://"):
            db_url = db_url.replace("postgresql+psycopg2://", "postgresql+pg8000://", 1)
        elif db_url.startswith("postgresql://"):
            db_url = db_url.replace("postgresql://", "postgresql+pg8000://", 1)

        engine = create_engine(db_url)
        # Test connection
        with engine.connect() as conn:
            pass
        return engine
    except Exception as e:
        raise ConnectionError(f"SQL connection failed: {str(e)}")


def get_mongo_client(db_url: str):
    """Returns (MongoClient, database_name)"""
    try:
        parsed = urlparse(db_url)
        # Extract DB name from URL path  e.g. /mydb
        db_name = parsed.path.lstrip("/").split("?")[0]
        if not db_name:
            raise ValueError("No database name found in MongoDB URL. Add /dbname at the end.")
        
        client = MongoClient(
            db_url,
            serverSelectionTimeoutMS=5000,
            tlsCAFile=certifi.where(),
            tlsDisableOCSPEndpointCheck=True,
        )
        # Test connection
        client.server_info()
        return client, db_name
    except Exception as e:
        raise ConnectionError(f"MongoDB connection failed: {str(e)}")