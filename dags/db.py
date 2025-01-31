from datetime import datetime
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator


# Function to read data from Postgres using PostgresHook
def read_from_postgres(**kwargs):
    hook = PostgresHook(postgres_conn_id='postgres_default')
    sql = "SELECT * FROM your_table LIMIT 5;"  # Replace 'your_table' with your table name
    connection = hook.get_conn()
    cursor = connection.cursor()
    cursor.execute(sql)
    result = cursor.fetchall()
    print(f"Data from Postgres:")
    for r in result:
        print(r)

    # Push the result to XCom for downstream tasks
    ti = kwargs['ti']
    ti.xcom_push(key='postgres_data', value=result)


# Function to write data into Postgres using PostgresHook
def write_to_postgres(**kwargs):
    hook = PostgresHook(postgres_conn_id='postgres_default')
    sql = "INSERT INTO your_table (column1, column2) VALUES (%s, %s);"  # Replace with your table and columns
    data_to_insert = ('tg', 'jhkuh')  # Example data to insert
    connection = hook.get_conn()
    cursor = connection.cursor()
    cursor.execute(sql, data_to_insert)
    connection.commit()
    print("Data successfully written to Postgres.")


# Define the DAG
with DAG(
    dag_id="postgres_read_write_dag",
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:

    # Task 1: Create Table (if not exists)
    create_table_task = PostgresOperator(
        task_id="create_table_task",
        postgres_conn_id="postgres_default",
        sql="""
        CREATE TABLE IF NOT EXISTS your_table (
            id SERIAL PRIMARY KEY,
            column1 TEXT,
            column2 TEXT
        );
        """,
    )

    # Task 2: Write Data to Postgres
    write_data_task = PythonOperator(
        task_id="write_data_task",
        python_callable=write_to_postgres,
        provide_context=True,
    )

    # Task 3: Read Data from Postgres
    read_data_task = PythonOperator(
        task_id="read_data_task",
        python_callable=read_from_postgres,
        provide_context=True,
    )

    # Define task dependencies
    create_table_task >> write_data_task >> read_data_task
