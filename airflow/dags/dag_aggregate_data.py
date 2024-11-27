from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
from scripts.aggregate_data import main

default_args = {
    'owner': 'airflow',
    'retries': 1,
}

with DAG(
    dag_id='dag_aggregate_data',
    default_args=default_args,
    start_date=datetime(2024, 11, 1),
    schedule='@daily',


) as dag:

    start = EmptyOperator(task_id="start")

    dag_aggregate_data = PythonOperator(
        task_id='dag_aggregate_data',
        python_callable=main, 
    )

    end = EmptyOperator(task_id="end")


(start >> dag_aggregate_data >> end)

