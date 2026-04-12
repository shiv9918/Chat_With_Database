from sqlalchemy import inspect
from db_manager import get_sql_engine, get_mongo_client, detect_db_type


def extract_sql_schema(db_url: str) -> dict:
	"""Extract table and column names from an SQL database."""
	engine = get_sql_engine(db_url)
	inspector = inspect(engine)

	schema = {}
	for table_name in inspector.get_table_names():
		columns = inspector.get_columns(table_name)
		schema[table_name] = [col["name"] for col in columns]

	return schema


def extract_mongo_schema(db_url: str) -> dict:
	"""Infer MongoDB schema from collection names and sample documents."""
	client, db_name = get_mongo_client(db_url)
	db = client[db_name]

	try:
		schema = {}
		for collection_name in db.list_collection_names():
			sample_doc = db[collection_name].find_one()
			if sample_doc:
				schema[collection_name] = list(sample_doc.keys())
			else:
				schema[collection_name] = []

		return schema
	finally:
		client.close()


def extract_schema(db_url: str) -> dict:
	"""Return database type and extracted schema for supported backends."""
	db_type = detect_db_type(db_url)

	if db_type == "sql":
		schema = extract_sql_schema(db_url)
	elif db_type == "mongodb":
		schema = extract_mongo_schema(db_url)
	else:
		raise ValueError(f"Unsupported db_type: {db_type}")

	return {
		"db_type": db_type,
		"schema": schema,
	}
