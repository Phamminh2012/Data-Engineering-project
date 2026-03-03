# Import Airflow here
import datetime

from airflow import DAG
from airflow.operators import PythonOperator
from airflow.operators import EmptyOperator

# Define DAG

with DAG(
    dag_id="Driver DAG",
    start_date=datetime.datetime(2010, 1, 1),
) as dag:
    start = EmptyOperator(task_id="to_be_filled")