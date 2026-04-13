from airflow.sdk import dag, task
import datetime
from scraping import *
from upload_data import upload, upload_csv
from transform import *
from skills_addition import do_skill_tagging_jsearch
from aggregations import do_job_count, do_skills_count, do_regression, do_job_description_wordcloud, do_topic_modeling

@dag("job_scraper", schedule=None, start_date=datetime.datetime(2026, 1, 1), catchup=False)
def the_driver():

    @task(task_id="MCF-Scrape")
    def mcf():
        return mcf_scrape("software development", limit=100, n_pages = 5) # First 100 jobs
    
    @task(task_id="JSearch-Scrape")
    def jSearch():
        return fetch_jsearch_jobs(query = "software development job in singapore", num_pages = 5)

    @task(task_id="Add-Skills-JSearch")
    def addjSkills(dummy):
        return do_skill_tagging_jsearch("jsearch")
    
    @task
    def upload_raw(db_name, col_name, f_path):
        return upload(db_name, col_name, f_path)

    @task(task_id="Transform-MCF-Mongo")
    def transformMCFOnMongo(something):
        return transformMCF(something)
    
    @task(task_id="Transform-JSEARCH-Mongo")
    def transformJSEARCHOnMongo(something):
        return transformJSearch(something)

    @task(task_id="Load-SSOC-Mapping")
    def load_ssoc():
        return upload_csv("raw_api_result", "ssoc_to_iso", "/opt/airflow/dags/SSOC to ISO.csv")

    @task(task_id="Load-ONET-Zone-Mapping")
    def load_onet_zone():
        return upload_csv("raw_api_result", "onet_zone_mappings", "/opt/airflow/dags/ONET ZONE MAPPINGS.csv")

    @task(task_id="Load-OSTAR-Mapping")
    def load_ostar():
        return upload_csv("raw_api_result", "ostar_to_iso", "/opt/airflow/dags/OSTAR to ISO.csv")

    @task(task_id="Load-SSEC-Mapping")
    def load_ssec():
        return upload_csv("raw_api_result", "ssec_mappings", "/opt/airflow/dags/SSEC MAPPINGS.csv")

    @task(task_id="Job-Count-Aggregation")
    def job_count_agg(dummy):
        return do_job_count(dummy)

    @task(task_id="Skills-Count-Aggregation")
    def skills_count_agg(dummy):
        return do_skills_count(dummy)

    @task(task_id="Regression-Analysis")
    def regression_agg(dummy):
        return do_regression(dummy)

    @task(task_id="WordCloud-JobDescription")
    def wordcloud_agg(dummy):
        return do_job_description_wordcloud(dummy)

    @task(task_id="Topic-Modeling")
    def topic_modeling_agg(dummy):
        return do_topic_modeling(dummy)

    m = mcf()
    j = jSearch()
    raw_db_name = "raw_api_result"

    ssoc = load_ssoc()
    zone = load_onet_zone()
    ostar = load_ostar()
    ssec = load_ssec()

    m_u = upload_raw(raw_db_name, "mcf", m)
    j_u = upload_raw(raw_db_name, "jsearch", j)
    
    j_skills = addjSkills(j_u)

    t_m = transformMCFOnMongo(m_u)
    t_j = transformJSEARCHOnMongo(j_skills)

    t_m.set_upstream(ssoc)
    t_m.set_upstream(ssec)
    t_j.set_upstream(zone)
    t_j.set_upstream(ostar)

    agg_j = job_count_agg(t_m)
    agg_s = skills_count_agg(t_m)
    agg_r = regression_agg(t_m)
    agg_w = wordcloud_agg(t_m)
    agg_t = topic_modeling_agg(t_m)

    # Ensure aggregations run after both transformations
    agg_j.set_upstream(t_j)
    agg_s.set_upstream(t_j)
    agg_w.set_upstream(t_j)
    agg_t.set_upstream(t_j)

the_driver()