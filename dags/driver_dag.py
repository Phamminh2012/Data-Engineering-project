# Import Airflow here

# Define DAG
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

from reed_import import get_from_reed  as data_1
from rapidapi_import import fetch_jsearch_jobs as data_2
from azuna_import import adzuna_import as data_3
from upload_data import upload as upload_data

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
   
    get_public_data = PythonOperator(
        task_id='get_reed_data',
        python_callable=data_1,
    )
    
    get_telegram_data = PythonOperator(
       task_id='get_azuna_data',
       python_callable=data_3
   )
   
    upload_data_gcs = PythonOperator(
        task_id='upload_to_gcs',
        python_callable=upload_data
    )
    
    # 4. Set dependencies
    # The '>>' operator tells Airflow the order of execution. 
    # Here, hello_task must finish successfully before world_task is allowed to start.
    get_public_data >> upload_data_gcs

  