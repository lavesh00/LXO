import cx_Oracle


class SQLAgent:
    def __init__(self, connection, table_name):
        self.connection = connection
        self.table_name = table_name

def insert_data(self, data):
    """Insert structured data into Oracle."""
    try:
        cursor = self.connection.cursor()
        columns = ", ".join(data.keys())
        values = ", ".join([f":{key}" for key in data.keys()])
        query = f"INSERT INTO {self.table_name} ({columns}) VALUES ({values})"
        cursor.execute(query, data)
        self.connection.commit()
        cursor.close()
        return {"message": f"Data inserted into Oracle table '{self.table_name}' successfully!"}
    except cx_Oracle.Error as e:
        return {"error": f"Oracle Error: {e}"}
