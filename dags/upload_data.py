from pymongo import MongoClient
import json
import os

# Local MongoDB running on host machine
uri = "mongodb://host.docker.internal:27017"

def upload():
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
        db = client["Group_project"]

        # Collections
        collection_rapid = db["rapid_api"]
        collection_adzuna = db["adzuna_api"]

        with open("/opt/airflow/data/raw/job_search.json") as f:
            rapid_data = json.load(f)

        with open("/opt/airflow/data/raw/adzuna_jobs.json") as f:
            adzuna_data = json.load(f)

        # insert
        if rapid_data:
            collection_rapid.insert_many(rapid_data)

        if adzuna_data:
            collection_adzuna.insert_many(adzuna_data)

        print("Data inserted successfully!")

    except Exception as e:
        print("Error:", e)
        raise Exception(e)

    finally:
        client.close()


