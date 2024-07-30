from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime
import pendulum

# Define the local timezone
local_tz = pendulum.timezone("Asia/Seoul")

# Define the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 7, 25, tzinfo=local_tz),
    'retries': 1,
}

with DAG(
    dag_id='test_dummy_dag_update_20_18',
    default_args=default_args,
    description='A simple test DAG',
    schedule_interval='@daily',
    catchup=False,
) as dag:
    
    # Define the dummy task
    dummy_task = DummyOperator(
        task_id='dummy_task',
    )

    # Set task dependencies if needed
    dummy_task
