import cx_Oracle
from pymongo import MongoClient

def get_oracle_connection(user, password, host, port, service):
    """Get Oracle connection using SQL*Plus."""
    try:
        # Establish the connection using cx_Oracle with the given details
        dsn = cx_Oracle.makedsn(host, port, service_name=service)
        connection = cx_Oracle.connect(user, password, dsn)
        return connection
    except cx_Oracle.Error as e:
        print(f"Error connecting to Oracle: {e}")
        return None

def get_mongo_connection(uri, db_name):
    """Connect to MongoDB."""
    try:
        client = MongoClient(uri)
        db = client[db_name]
        collection = db["DBMS"]
        print("MongoDB connection established successfully!")
        return collection
    except Exception as e:
        print(f"Error connecting to MongoDB: {e}")
        return None
