from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from ufc_etl.main import main_runner

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 13),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    'weekly_ufc_dag',
    default_args = default_args,
    description='A simple weekly ufc DAG to fetch ufc data',
    schedule="@weekly",
    catchup=False
) as dag:
    run_etl = PythonOperator(
        task_id = 'run_etl_script',
        python_callable=main_runner, 
    )

    run_etl



