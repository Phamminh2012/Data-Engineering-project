from airflow.sdk import dag, task
import datetime
from scraping import *
from upload_data import upload
from transform import *

@dag("job_scraper", schedule=None, start_date=datetime.datetime(2026, 1, 1), catchup=False)
def the_driver():

    @task(task_id="MCF-Scrape")
    def mcf():
        return mcf_scrape("it job", 50)
    
    @task(task_id="Adzuna-Scrape")
    def adzuna():
        return adzuna_import("software job")
    
    @task(task_id="JSearch-Scrape")
    def jSearch():
        return fetch_jsearch_jobs(query = "software job in singapore")
    
    @task
    def upload_raw(db_name, col_name, f_path):
        return upload(db_name, col_name, f_path)

    @task(task_id="Transform-MCF-Mongo")
    def transformMCFOnMongo(something):
        return transformMCF(something)

    @task(task_id="Transform-Adzuma-Mongo")
    def transformAdzumaOnMongo(something):
        return transformAdzuma(something)
    
    @task(task_id="Transform-JSEARCH-Mongo")
    def transformJSEARCHOnMongo(something):
        return transformJSearch(something)

    m = mcf()
    a = adzuna()
    j = jSearch()
    raw_db_name = "raw_api_result"

    upload_raw(raw_db_name,"mcf", m)
    upload_raw(raw_db_name,"adzuma", a)
    upload_raw(raw_db_name, "jsearch", j)

    transformMCFOnMongo(m)
    transformAdzumaOnMongo(a)
    transformJSEARCHOnMongo(j)

the_driver()