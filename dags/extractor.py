from airflow.sdk import task, dag
from datetime import datetime, timedelta
from publicapi_import import get_from_job_portal
from rapidapi_import import fetch_jsearch_jobs
from telegram_import import telegram_import
from send_telegram import send_myself

@dag("extractor", schedule=timedelta(minutes=10), start_date=datetime(2026, 1, 1), catchup=False)

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
    def do_a_summary(a, b, c):
        return f"There are {len(a)} entries from RAPID API and {len(b)} entries from Public API, and {len(c)} entries from Telegram channels."
    
    @task
    def send_something(string_to_say):
        send_myself(string_to_say)
        send_myself(f"This is correct as of {datetime.now().isoformat()}")
        return None
    
    # get_rapid_data()
    a = get_rapid_data()
    b = get_public_data()
    c = get_telegram_data()
    string_to_say = do_a_summary(a, b, c)
    send_something(string_to_say)

extractor()