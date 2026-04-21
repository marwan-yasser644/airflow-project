from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'marwan',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'agri_iot_etl_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
) as dag:

    # 1. simulate data
    simulate_data = BashOperator(
        task_id='simulate_data',
        bash_command='python "/FULL/PATH/simulate_data.py"',
    )

    # 2. extract to HDFS
    extract_to_hdfs = BashOperator(
        task_id='extract_to_hdfs',
        bash_command='spark-submit "/FULL/PATH/extraction_to_hdfs.py"',
    )

    # 3. transform (gold layer)
    transform_data = BashOperator(
        task_id='transform_data',
        bash_command='spark-submit "/FULL/PATH/transformation_spark.py"',
    )

    # 4. load to snowflake
    load_to_snowflake = BashOperator(
        task_id='load_to_snowflake',
        bash_command='spark-submit "/FULL/PATH/load_to_snowflake.py"',
    )

    simulate_data >> extract_to_hdfs >> transform_data >> load_to_snowflake