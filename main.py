import cx_Oracle
from pymongo import MongoClient
import os


def prompt_user_for_table_creation(connection):
    """Prompt the user to create a table dynamically in Oracle."""
    table_name = input("Enter the name of the table to create: ")
    if not table_name.isidentifier():
        print("Invalid table name. Please use a valid identifier.")
        return None, None
    columns = []
    while True:
        column_name = input("Enter the name of the column (or type 'done' to finish): ")
        if column_name.lower() == "done":
            if len(columns) == 0:
                print("You must add at least one column.")
                continue
            break
        if not column_name.isidentifier():
            print("Invalid column name. Please use a valid identifier.")
            continue

        data_type = input(f"Enter the data type for column {column_name} (e.g., VARCHAR2(50), NUMBER, DATE): ")
        columns.append(f"{column_name} {data_type}")

    create_table_query = f"CREATE TABLE {table_name} ({', '.join(columns)})"

    try:
        cursor = connection.cursor()
        cursor.execute(create_table_query)
        connection.commit()
        cursor.close()
        print(f"Table '{table_name}' created successfully!")
        return table_name, [col.split()[0] for col in columns]  # Return table name and column names
    except cx_Oracle.Error as e:
        print(f"Error creating table: {e}")
        return None, None


def collect_data():
    """Collect data for insertion."""
    print("Enter data for insertion:")
    data = {}
    while True:
        key = input("Enter attribute name (or 'done' to finish): ")
        if key.lower() == "done":
            break
        value = input(f"Enter value for {key}: ")
        data[key] = value
    return data


def route_data(data, column_names, oracle_connection, mongo_collection, table_name):
    """Route data to Oracle or MongoDB."""
    if all(key in column_names for key in data.keys()):
        # All attributes match Oracle table schema
        try:
            cursor = oracle_connection.cursor()
            columns = ", ".join(data.keys())
            values = ", ".join([f":{key}" for key in data.keys()])
            query = f"INSERT INTO {table_name} ({columns}) VALUES ({values})"
            cursor.execute(query, data)
            oracle_connection.commit()
            cursor.close()
            return {"message": f"Data inserted into Oracle table '{table_name}' successfully!"}
        except cx_Oracle.Error as e:
            return {"error": f"Oracle Error: {e}"}
    else:
        # Attributes don't match, store in MongoDB
        try:
            mongo_collection.insert_one(data)
            return {"message": "Data inserted into MongoDB successfully!"}
        except Exception as e:
            return {"error": f"MongoDB Error: {e}"}


def view_oracle_tables(connection):
    """View all Oracle tables and their data."""
    try:
        cursor = connection.cursor()
        cursor.execute("SELECT table_name FROM user_tables")
        tables = cursor.fetchall()
        if not tables:
            print("No tables found in Oracle.")
            return

        print("Available tables in Oracle:")
        for i, (table_name,) in enumerate(tables, start=1):
            print(f"{i}. {table_name}")

        choice = input("Enter the table number to view its data (or 'back' to return): ").strip()
        if choice.lower() == "back":
            return

        try:
            choice = int(choice) - 1
            selected_table = tables[choice][0]
            cursor.execute(f"SELECT * FROM {selected_table}")
            rows = cursor.fetchall()
            if rows:
                print(f"Data in table '{selected_table}':")
                for row in rows:
                    # Trim spaces for CHAR columns
                    trimmed_row = tuple(value.strip() if isinstance(value, str) else value for value in row)
                    print(trimmed_row)
            else:
                print(f"Table '{selected_table}' is empty.")
        except (IndexError, ValueError):
            print("Invalid choice.")
    except cx_Oracle.Error as e:
        print(f"Error fetching tables: {e}")


def view_mongo_data(collection):
    """View all data in MongoDB collection."""
    try:
        data = list(collection.find())
        if data:
            print("Data in MongoDB:")
            for document in data:
                document["_id"] = str(document["_id"])  # Convert ObjectId to string
                print(document)
        else:
            print("No data found in MongoDB.")
    except Exception as e:
        print(f"Error fetching MongoDB data: {e}")


def main():
    # Get Oracle and MongoDB connection details
    oracle_user = input("Enter Oracle username: ")
    oracle_password = input("Enter Oracle password: ")
    oracle_host = input("Enter Oracle host (e.g., localhost): ")
    oracle_port = input("Enter Oracle port (e.g., 1521): ")
    oracle_service = input("Enter Oracle service name (e.g., xe): ")

    mongo_uri = input("Enter MongoDB URI (e.g., mongodb://localhost:27017/): ")
    mongo_db_name = input("Enter MongoDB database name: ")

    # Establish connections
    try:
        oracle_connection = cx_Oracle.connect(
            oracle_user,
            oracle_password,
            cx_Oracle.makedsn(oracle_host, oracle_port, service_name=oracle_service),
        )
    except cx_Oracle.DatabaseError as e:
        print(f"Error connecting to Oracle: {e}")
        return

    try:
        mongo_client = MongoClient(mongo_uri)
        mongo_collection = mongo_client[mongo_db_name]["DBMS"]
    except Exception as e:
        print(f"Error connecting to MongoDB: {e}")
        return

    table_name, column_names = None, None

    while True:
        print("\nMain Menu:")
        print("1. Create a new table in Oracle")
        print("2. Insert data")
        print("3. View Oracle tables and data")
        print("4. View MongoDB data")
        print("5. Exit")

        choice = input("Enter your choice: ").strip()

        if choice == "1":
            table_name, column_names = prompt_user_for_table_creation(oracle_connection)
        elif choice == "2":
            if not table_name:
                print("Please create a table first.")
                continue
            data = collect_data()
            result = route_data(data, column_names, oracle_connection, mongo_collection, table_name)
            print(result)
        elif choice == "3":
            view_oracle_tables(oracle_connection)
        elif choice == "4":
            view_mongo_data(mongo_collection)
        elif choice == "5":
            print("Exiting the program.")
            break
        else:
            print("Invalid choice. Please try again.")


if __name__ == "__main__":
    main()
