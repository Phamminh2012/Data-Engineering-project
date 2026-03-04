from airflow.sdk import task, dag
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
        
    a = get_rapid_data()
    b = get_public_data()
    c = get_telegram_data()

extractor()