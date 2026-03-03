from airflow.sdk import task, dag
from datetime import datetime
from publicapi_import import get_from_job_portal  as data_1
from rapidapi_import import fetch_jsearch_jobs as data_2

@dag("extractor", schedule=None, start_date=datetime(2026, 1, 1), catchup=False)

def extractor():
    @task
    def get_rapid_data():
        return data_2()
    
    @task
    def get_public_data():
        return data_1()
    get_rapid_data()
    get_public_data()

extractor()