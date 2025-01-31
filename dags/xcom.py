from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

# Function to push data to XCom
def push_xcom_example(**kwargs):
    value_to_push = "Hello from XCom!"
    kwargs['ti'].xcom_push(key='my_key', value=value_to_push)
    print(f"Value pushed to XCom: {value_to_push}")

# Function to pull data from XCom
def pull_xcom_example(**kwargs):
    # Pull value using the task instance
    pulled_value = kwargs['ti'].xcom_pull(key='my_key', task_ids='push_xcom_task')
    print(f"Value pulled from XCom: {pulled_value}")

# Define the DAG
with DAG(
    dag_id="xcom_example_dag",
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:
    
    # Task to push data to XCom
    push_xcom_task = PythonOperator(
        task_id="push_xcom_task",
        python_callable=push_xcom_example,
        provide_context=True,
    )

    # Task to pull data from XCom
    pull_xcom_task = PythonOperator(
        task_id="pull_xcom_task",
        python_callable=pull_xcom_example,
        provide_context=True,
    )

    # Set task dependencies
    push_xcom_task >> pull_xcom_task
