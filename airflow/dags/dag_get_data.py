from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime
from scripts.get_data import main

default_args = {
    'owner': 'airflow',
    'retries': 1,
}

with DAG(
    dag_id='dag_get_data',
    schedule_interval='00 8 * * *',  # Executar todos os dias às 8:00 AM
    default_args=default_args,
    start_date=datetime(2024, 11, 1),
    catchup=False

) as dag:

    start = EmptyOperator(task_id="start")

    get_data = PythonOperator(
        task_id='get_data',
        python_callable=main, 
    )

    trigger_transform = TriggerDagRunOperator(
        task_id='trigger_transform',
        trigger_dag_id='dag_transform_data'
    )

    end = EmptyOperator(task_id="end")


(start >> get_data >> trigger_transform >> end)
