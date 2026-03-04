from airflow.sdk import task, dag
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from datetime import datetime, timedelta
from publicapi_import import get_from_job_portal
from rapidapi_import import fetch_jsearch_jobs
from telegram_import import telegram_import

@dag("extractor", schedule=None, start_date=datetime(2026, 1, 1), catchup=False)

def extractor():
    @task
    def get_rapid_data():
        return fetch_jsearch_jobs()
    
    @task
    def get_public_data():
        return get_from_job_portal()
    
    @task
    def get_telegram_data():
        return telegram_import()
    
    @task
    def upload_to_gcs(file_path, bucket_name, dest_blob):
        gcs_hook = GCSHook(connection_id="google_cloud_default")
        gcs_hook.upload(bucket_name=bucket_name, object_name=dest_blob, filename=file_path)

    a = get_rapid_data()
    b = get_public_data()
    c = get_telegram_data()
    upload_to_gcs(a, "job-data-bucket-raw", "raw_rapidapi_data.json")
    upload_to_gcs(b, "job-data-bucket-raw", "raw_publicapi_data.json")
    upload_to_gcs(c, "job-data-bucket-raw", "raw_telegram_data.json")

extractor()