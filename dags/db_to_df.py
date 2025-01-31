from datetime import datetime
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
import pandas as pd

# Define the PostgreSQL connection parameters
POSTGRES_CONN_ID = 'postgres_default'
TABLE_NAME = 'your_table'

def read_postgres_to_dataframe():
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = hook.get_conn()
    
    # Query to fetch data from the table
    query = f"SELECT * FROM {TABLE_NAME};"
    
    # Load data into a Pandas DataFrame
    df = pd.read_sql_query(query, conn)
    conn.close()
    print(df.head())
    return df

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

dag = DAG(
    dag_id="postgres_to_dataframe",
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
)

read_data_task = PythonOperator(
    task_id='read_postgres_to_dataframe',
    python_callable=read_postgres_to_dataframe,
    dag=dag,
)

read_data_task