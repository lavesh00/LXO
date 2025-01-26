from pymongo import MongoClient


class NoSQLAgent:
    def __init__(self, collection):
        self.collection = collection

    def insert_data(self, data):
        try:
            self.collection.insert_one(data)
            return {"message": "Data inserted into MongoDB successfully!"}
        except Exception as e:
            return {"error": str(e)}

    def fetch_data(self):
        try:
            data = list(self.collection.find())
            for document in data:
                if "_id" in document:
                    document["_id"] = str(document["_id"])  # Convert ObjectId to string
            return data
        except Exception as e:
            return {"error": str(e)}
