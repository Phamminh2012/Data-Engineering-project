from pymongo import MongoClient
from pymongo.server_api import ServerApi
import json
import os
from dotenv import load_dotenv

load_dotenv()

uri = os.getenv("URI")

def upload():
    client = MongoClient(
        uri,
        server_api=ServerApi('1'),
        serverSelectionTimeoutMS=30000,
        connectTimeoutMS=30000,
        socketTimeoutMS=60000,
    )

    try:
        client.admin.command('ping')
        print("Connected to MongoDB!")

        # Choose database
        db = client["Data_engineer_project"]

        # Collections
        collection_rapid = db["rapid_api"]
        collection_adzuna = db["adzuna_api"]

        with open("/opt/airflow/data/raw/job_search.json") as f:
            rapid_data = json.load(f)

        with open("/opt/airflow/data/raw/adzuna_jobs.json") as f:
            adzuna_data = json.load(f)

        collection_rapid.insert_many(rapid_data)
        collection_adzuna.insert_many(adzuna_data)
        print("Data inserted successfully!")

    except Exception as e:
        print(f"Error: {e}")
        raise 

    finally:
        client.close()

