from sqlalchemy import text
from pymongo import MongoClient
from db_manager import get_sql_engine, get_mongo_client, detect_db_type
import re
import ast


def is_query_safe(query: str, db_type: str) -> bool:
    """Block any destructive operations"""
    dangerous_keywords = [
        "DROP", "DELETE", "UPDATE", "INSERT", "ALTER",
        "TRUNCATE", "EXEC", "EXECUTE", "CREATE", "REPLACE"
    ]
    query_upper = query.upper()
    
    for keyword in dangerous_keywords:
        # Use word boundary to avoid false positives
        if re.search(rf'\b{keyword}\b', query_upper):
            return False
    return True


def execute_sql_query(db_url: str, query: str) -> dict:
    """Executes SQL query and returns results"""
    if not is_query_safe(query, "sql"):
        raise PermissionError("Only SELECT queries are allowed.")
    
    engine = get_sql_engine(db_url)
    
    with engine.connect() as conn:
        result = conn.execute(text(query))
        
        # Get column names from result
        columns = list(result.keys())
        
        # Convert rows to list of dicts
        rows = []
        for row in result.fetchall():
            rows.append(dict(zip(columns, row)))
    
    return {
        "columns": columns,
        "rows": rows,
        "count": len(rows)
    }


def execute_mongo_query(db_url: str, query: str) -> dict:
    """
    Executes PyMongo query string safely.
    Supports: db.collection.find({}) and db.collection.aggregate([])
    """
    if not is_query_safe(query, "mongodb"):
        raise PermissionError("Only read operations are allowed.")
    
    client, db_name = get_mongo_client(db_url)
    db = client[db_name]
    
    try:
        # Parse: db.users.find({"age": {"$gt": 25}})
        # Extract collection name and operation
        cleaned_query = query.strip().rstrip(";")
        pattern = r'^db\.(\w+)\.(find|aggregate|find_one|findOne|count|count_documents|estimated_document_count)\((.*)\)$'
        match = re.match(pattern, cleaned_query, re.DOTALL)
        
        if not match:
            raise ValueError(f"Unrecognized query format: {query}")
        
        collection_name = match.group(1)
        operation = match.group(2)
        operation = operation.lower()
        args_str = match.group(3).strip()
        
        collection = db[collection_name]
        
        if not args_str:
            args = None
        else:
            try:
                args = ast.literal_eval(args_str)
            except Exception as e:
                raise ValueError(f"Invalid MongoDB query arguments: {str(e)}")
        
        if operation == "find":
            if isinstance(args, dict):
                cursor = collection.find(args)
            elif isinstance(args, (list, tuple)) and len(args) == 2:
                cursor = collection.find(args[0], args[1])
            else:
                cursor = collection.find({})
            
            rows = []
            for doc in cursor.limit(100):  # Safety limit
                doc["_id"] = str(doc["_id"])  # Convert ObjectId to string
                rows.append(doc)
        
        elif operation == "aggregate":
            pipeline = args if isinstance(args, list) else []
            rows = []
            for doc in collection.aggregate(pipeline):
                if "_id" in doc:
                    doc["_id"] = str(doc["_id"])
                rows.append(doc)
        
        elif operation in ("findone", "find_one"):
            doc = collection.find_one(args if args else {})
            if doc:
                doc["_id"] = str(doc["_id"])
            rows = [doc] if doc else []
        
        elif operation in ("count", "count_documents"):
            count = collection.count_documents(args if args else {})
            rows = [{"count": count}]

        elif operation == "estimated_document_count":
            count = collection.estimated_document_count()
            rows = [{"count": count}]
        
        columns = list(rows[0].keys()) if rows else []
        
        return {
            "columns": columns,
            "rows": rows,
            "count": len(rows)
        }
    
    finally:
        client.close()


def execute_query(db_url: str, query: str) -> dict:
    """Auto-detects DB type and executes"""
    db_type = detect_db_type(db_url)
    
    if db_type == "sql":
        return execute_sql_query(db_url, query)
    else:
        return execute_mongo_query(db_url, query)