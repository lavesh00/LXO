import os
from get_connection import get_oracle_connection, get_mongo_connection
from prompt_helpers import prompt_user_for_table_creation
from arbitration_agent import ArbitrationAgent

def check_oracle_client():
    """Check if Oracle Instant Client is installed (for fallback check)."""
    if "ORACLE_HOME" not in os.environ:
        print("Oracle Instant Client is not installed or not configured. Please install it first.")
        return False
    return True

def main():
    # Ensure Oracle environment is set up
    if not check_oracle_client():
        return

    # Get Oracle and MongoDB connection details from user
    oracle_user = input("Enter Oracle username: ")
    oracle_password = input("Enter Oracle password: ")
    oracle_host = input("Enter Oracle host (e.g., localhost): ")
    oracle_port = input("Enter Oracle port (e.g., 1521): ")
    oracle_service = input("Enter Oracle service name (e.g., xe): ")

    mongo_uri = input("Enter MongoDB URI (e.g., mongodb://localhost:27017/): ")
    mongo_db_name = input("Enter MongoDB database name: ")
    mongo_collection_name = input("Enter MongoDB collection name: ")

    # Establish connections
    oracle_connection = get_oracle_connection(oracle_user, oracle_password, oracle_host, oracle_port, oracle_service)
    mongo_collection = get_mongo_connection(mongo_uri, mongo_db_name)

    if not oracle_connection or not mongo_collection:
        print("Failed to establish connections.")
        return

    # Allow user to create a table in Oracle
    table_name = prompt_user_for_table_creation(execute_sqlplus_command)

    # Allow user to input data
    print("Enter data for insertion (structured data will go to Oracle, unstructured to MongoDB):")
    data = {}
    while True:
        key = input("Enter key (or 'done' to finish): ")
        if key.lower() == "done":
            break
        value = input(f"Enter value for {key}: ")
        data[key] = value

    # Process and route data to SQL or NoSQL
    arbitration_agent = ArbitrationAgent(data)
    result = arbitration_agent.route_to_sql_or_nosql(oracle_connection, mongo_collection, table_name)

    print(result)

if __name__ == "__main__":
    main()
