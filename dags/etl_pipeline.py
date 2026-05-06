from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

from scripts.simulateRealWorldData import simulate
from scripts.ExtractionToHDFS import extract
from scripts.TransformationSpark import transform
from scripts.LoadToSnowFlake import load_to_snowflake

default_args = {
    'owner': 'marwan',
    'start_date': datetime(2024, 1, 1)
}

with DAG(
    dag_id='etl_pipeline',
    schedule_interval='@daily',
    catchup=False,
    default_args=default_args
) as dag:

    task1 = PythonOperator(
        task_id='simulate_data',
        python_callable=simulate
    )

    task2 = PythonOperator(
        task_id='extract_to_hdfs',
        python_callable=extract
    )

    task3 = PythonOperator(
        task_id='transform_spark',
        python_callable=transform
    )

    task4 = PythonOperator(
        task_id='load_to_snowflake',
        python_callable=load_to_snowflake
    )

    task1 >> task2 >> task3 >> task4