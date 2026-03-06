from google.cloud import storage
import json

def upload():
    bucket_name = "data_engineer_nus"
    paths = ["/opt/airflow/data/raw/telegram_search.json", "/opt/airflow/data/raw/job-board-public.json"]

    
    client = storage.Client(project="is3107-job-database")
    bucket = client.bucket(bucket_name)

    for file_path in paths:
        try:
            with open(file_path, 'r', encoding='utf-8') as file:
                data = json.load(file)
        except FileNotFoundError:
            print(f"Error: {file_path} was not found. Skipping.")
            continue  # skip to next file
        except json.JSONDecodeError:
            print(f"Error: Could not decode JSON from {file_path}. Skipping.")
            continue  # skip to next file

        blob_name = file_path  
        blob = bucket.blob(blob_name)
        blob.upload_from_string(
            json.dumps(data, ensure_ascii=False, indent=2),
            content_type="application/json"
        )
        print(f"Uploaded {file_path} to gs://{bucket_name}/{blob_name}")