from airflow.sdk import dag, task
import datetime
from scraping import *
from upload_data import upload
from transform import *
from skills_addition import do_skill_tagging_jsearch
from aggregations import *

@dag("job_scraper", schedule=None, start_date=datetime.datetime(2026, 1, 1), catchup=False)
def the_driver():

    @task(task_id="MCF-Scrape")
    def mcf():
        return mcf_scrape("software development", limit=20, n_pages = 5) # First 100 jobs
    
    @task(task_id="JSearch-Scrape")
    def jSearch():
        return fetch_jsearch_jobs(query = "software development job in singapore", num_pages = 5)

    @task(task_id="Add-Skills-JSearch")
    def addjSkills(json_input):
        return do_skill_tagging_jsearch(json_input)
    
    @task
    def upload_raw(db_name, col_name, f_path):
        return upload(db_name, col_name, f_path)

    @task(task_id="Transform-MCF-Mongo")
    def transformMCFOnMongo(something):
        return transformMCF(something)
    
    @task(task_id="Transform-JSEARCH-Mongo")
    def transformJSEARCHOnMongo(something):
        return transformJSearch(something)
    
    @task(task_id="Top-5-Skills")
    def findTop5(*args):
        return do_skills_count(None)
    
    @task(task_id="Count-Jobs")
    def countJobs(*args):
        return do_job_count(None)

    m = mcf()
    j = jSearch()
    j_skills = addjSkills(j)
    raw_db_name = "raw_api_result"

    m_u = upload_raw(raw_db_name,"mcf", m)
    j_u = upload_raw(raw_db_name, "jsearch", j_skills)

    t_m = transformMCFOnMongo(m_u)
    t_j = transformJSEARCHOnMongo(j_u)

    findTop5(t_m, t_j)
    countJobs(t_m, t_j)

the_driver()