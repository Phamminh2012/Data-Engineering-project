# Import Airflow here

# Define DAG
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator


from rapidapi_import import fetch_jsearch_jobs as data_2

from mcf_scrap import mcf_scrape as data_1
from upload_data import upload,upload_clean 
from transform_mcf import transform_mcf 
from transform_jsearch import transform_job_search
with DAG(
    dag_id='Testing_airflow_first_time',
    description='Testing call API',
    start_date=datetime(2026, 1, 1), # YYYY , MM, DD
    
    # --- SCHEDULE EXAMPLES ---
    # schedule='@daily',        # Preset: Runs once a day at midnight
    # schedule='@hourly',       # Preset: Runs at the beginning of every hour
    # schedule='0 12 * * *',    # Cron expression: Runs exactly at 12:00 PM every day
    schedule=None,
    
    # --- CATCHUP EXPLANATION ---
    # If catchup=True: Airflow looks at your start_date and automatically runs 
    # every missed scheduled interval between that date and right now (called "backfilling").
    # If catchup=False: It ignores the past and only runs from the current date onward.
    catchup=False,
    
    tags=['tutorial_4'],
)as dag:
   
    
    get_rapid_data = PythonOperator(
        task_id='get_rapid_data',
        python_callable=data_2,
    )
    
    
    get_mcf_data = PythonOperator(
       task_id='get_mcf_data',
       python_callable=data_1,
       op_kwargs={
        "keywords": "IT job",
        "limit": 30
        }
    )
    
    
    
    upload_mongo = PythonOperator(
        task_id='upload_to_mongo',
        python_callable=upload
    )
    

    transform_mcf_data = PythonOperator(
        task_id='transform_mcf_data',
        python_callable=transform_mcf,
        op_kwargs={
            "json_path":"/opt/airflow/data/raw/mcf_data.json"
        }
    )

    transform_jsearch= PythonOperator(
        task_id="transform_job_search",
        python_callable=transform_job_search,
        op_kwargs={"json_path":"/opt/airflow/data/raw/job_search.json",
                   "skill_csv":"/opt/airflow/data/raw/distinct_skills.csv"}
    )
    
    upload_mongo_clean = PythonOperator(
        task_id='upload_to_mongo_clean',
        python_callable=upload_clean
    )
    
    get_rapid_data >> upload_mongo 
    
    get_mcf_data >> upload_mongo

    upload_mongo >>transform_mcf_data >> upload_mongo_clean
    upload_mongo >> transform_jsearch >> upload_mongo_clean
   
   
   

  