from sql_agent import SQLAgent
from nosql_agent import NoSQLAgent


class ArbitrationAgent:
    def __init__(self, data):
        self.data = data

    def is_structured(self):
        """Check if data is structured based on required keys."""
        try:
            return all(isinstance(value, (str, int, float)) for value in self.data.values())
        except Exception as e:
            print(f"Error determining data structure: {e}")
            return False

    def route_to_sql_or_nosql(self, oracle_connection, mongo_collection, table_name):
        """Route data to Oracle (SQL) or MongoDB (NoSQL)."""
        if self.is_structured():
            sql_agent = SQLAgent(oracle_connection, table_name)
            return sql_agent.insert_data(self.data)
        else:
            nosql_agent = NoSQLAgent(mongo_collection)
            return nosql_agent.insert_data(self.data)
