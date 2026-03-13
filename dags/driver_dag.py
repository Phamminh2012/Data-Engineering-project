from airflow.sdk import dag, task
import datetime
from scraping import *
from upload_data import upload

@dag("job_scraper", schedule=None, start_date=datetime.datetime(2026, 1, 1), catchup=False)
def the_driver():

    @task(task_id="MCF-Scrape")
    def mcf():
        return mcf_scrape("software engineer", 50)
    
    @task(task_id="Adzuna-Scrape")
    def adzuna():
        return adzuna_import()
    
    @task(task_id="JSearch-Scrape")
    def jSearch():
        return fetch_jsearch_jobs(query = "software engineer in singapore")

    @task(task_id="GOV-SG-Scrape")
    def govSearch():
        return fetch_gov_sg_listings()
    
    @task
    def upload_raw(db_name, col_name, f_path):
        return upload(db_name, col_name, f_path)

    m = mcf()
    a = adzuna()
    j = jSearch()
    g = govSearch()
    raw_db_name = "raw_api_result"

    upload_raw(raw_db_name,"mcf", m)
    upload_raw(raw_db_name,"adzuma", a)
    upload_raw(raw_db_name, "jsearch", j)
    upload_raw(raw_db_name, "govsearch", g)

the_driver()