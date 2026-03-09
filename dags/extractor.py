from airflow.sdk import task, dag
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from datetime import datetime, timedelta
from rapidapi_import import fetch_jsearch_jobs
from azuna_import import adzuna_import
from reed_extractor import get_reed_listings
from mcf_scrape import mcf_scrape

@dag("extractor", schedule=None, start_date=datetime(2026, 1, 1), catchup=False)

def extractor():
    @task
    def get_rapid_data():
        return fetch_jsearch_jobs("IT job in Singapore")
    
    @task
    def get_from_adzuna():
        return adzuna_import("IT job") # Minh has fixed the search term
    @task
    def get_from_reed():
        return get_reed_listings("IT job", "london", 5) # This WILL raise an exception.
    
    @task
    def get_from_mcf():
        return mcf_scrape("IT job", 50)
    
    @task
    def upload_to_gcs(file_path, bucket_name, dest_blob):
        gcs_hook = GCSHook(connection_id="google_cloud_default") # Please get your connection ID!
        gcs_hook.upload(bucket_name=bucket_name, object_name=dest_blob, filename=file_path)

    a = get_rapid_data()
    b = get_from_adzuna()
    c = get_from_reed()
    d = get_from_mcf()


extractor()