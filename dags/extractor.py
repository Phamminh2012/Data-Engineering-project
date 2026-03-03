from airflow.sdk import task, dag
from datetime import datetime
from publicapi_import import get_from_job_portal
from rapidapi_import import fetch_jsearch_jobs

@dag("extractor", schedule=None, start_date=datetime(2026, 1, 1), catchup=False)

def extractor():
    @task
    def get_rapid_data():
        return fetch_jsearch_jobs()
    
    @task
    def get_public_data():
        return get_from_job_portal()
    
    @task
    def do_something(a_param, another_param):
        return "Yes."
    
    a = get_rapid_data()
    b = get_public_data()
    do_something(a, b) # The pythonic way is SO MUCH better!

extractor()