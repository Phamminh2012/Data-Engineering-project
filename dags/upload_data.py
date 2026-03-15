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
        
        collection_mcf = db["mcf_scrape"]

        with open("/opt/airflow/data/raw/job_search.json") as f:
            rapid_data = json.load(f)

        

        with open("/opt/airflow/data/raw/mcf_data.json") as f:
            mcf_data = json.load(f)

        # insert
        if rapid_data:
            collection_rapid.insert_many(rapid_data)

        

        if mcf_data:
            collection_mcf.insert_many(mcf_data)

        print("Data inserted successfully!")

    except Exception as e:
        print("Error:", e)
        raise Exception(e)

    finally:
        client.close()

def upload_clean():
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
        clean_data = db["Clean_data"]
        
        

        with open("/opt/airflow/data/processed/res_job_search.json") as f:
            rapid_data = json.load(f)

        

        with open("/opt/airflow/data/processed/mcf_data_processed.json") as f:
            mcf_data = json.load(f)

        # insert
        if rapid_data:
            clean_data.insert_many(rapid_data)

        

        if mcf_data:
            clean_data.insert_many(mcf_data)

        print("Data inserted successfully!")

    except Exception as e:
        print("Error:", e)
        raise Exception(e)

    finally:
        client.close()


