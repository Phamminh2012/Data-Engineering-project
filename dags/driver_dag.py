# Import Airflow here

# Define DAG
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator


from rapidapi_import import fetch_jsearch_jobs as data_2
from azuna_import import adzuna_import as data_3
from mcf_scrap import mcf_scrape as data_1
from upload_data import upload as upload_data


with DAG(
    dag_id='JobSearchAndAnalysis',
    description='IS3107 group project on job analytics',
    start_date=datetime(2026, 1, 1), # YYYY , MM, DD
    
    # --- SCHEDULE EXAMPLES ---
    # schedule='@daily',        # Preset: Runs once a day at midnight
    # schedule='@hourly',       # Preset: Runs at the beginning of every hour
    # schedule='0 12 * * *',    # Cron expression: Runs exactly at 12:00 PM every day
    schedule='@daily',
    
    # --- CATCHUP EXPLANATION ---
    # If catchup=True: Airflow looks at your start_date and automatically runs 
    # every missed scheduled interval between that date and right now (called "backfilling").
    # If catchup=False: It ignores the past and only runs from the current date onward.
    catchup=False,
    
    tags=['project'],
)as dag:
    
    get_rapid_data = PythonOperator(
        task_id='get_rapid_data',
        python_callable=data_2,
    )
    get_adzuna_data = PythonOperator(
       task_id='get_azuna_data',
       python_callable=data_3
    )
    
    get_mcf_data = PythonOperator(
       task_id='get_mcf_data',
       python_callable=data_1,
       op_kwargs={
        "keywords": "software engineer",
        "limit": 50
        }
    )

   
    
    upload_mongo = PythonOperator(
        task_id='upload_to_mongo',
        python_callable=upload_data
    )
    
    get_rapid_data >> upload_mongo 
    get_adzuna_data >> upload_mongo 
    get_mcf_data >> upload_mongo
    # 4. Set dependencies
    # The '>>' operator tells Airflow the order of execution. 
    # Here, hello_task must finish successfully before world_task is allowed to start.

   

  