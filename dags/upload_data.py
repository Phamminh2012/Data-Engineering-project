from pymongo import MongoClient
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
        collection = df[f"{col_name}"]

        with open(f_path) as f:
            data = json.load(f)
        
        if data:
            collection.insert_many(data)

        print("Data inserted successfully!")

    except Exception as e:
        print("Error:", e)
        raise Exception(e)

    finally:
        client.close()


