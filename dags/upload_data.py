from pymongo import MongoClient
from pymongo.errors import BulkWriteError
import json
import os

# Local MongoDB running on host machine
uri = "mongodb://host.docker.internal:27017"

def upload(db_name, col_name, f_path):
    client = MongoClient(
        uri,
        serverSelectionTimeoutMS=30000,
        connectTimeoutMS=30000,
        socketTimeoutMS=60000,
    )

    try:
        client.admin.command("ping")
        print("Connected to local MongoDB!")

        # Database
        db = client[f"{db_name}"]

        # Collections
        collection = db[f"{col_name}"]

        with open(f_path) as f:
            data = json.load(f)
        try:
            if data:
                collection.insert_many(data, ordered = False)
        except BulkWriteError as bwe:
            print("Apparently there were some duplicates...")

        print("Data inserted successfully!")

    except Exception as e:
        print("Error:", e)
        raise Exception(e)

    finally:
        client.close()


