import os
import re
import pandas as pd
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.empty import EmptyOperator


# Define constants
DIRECTORY_PATH = "/opt/airflow/files"  # Replace with the directory path
HEADER = ["emp_id", "name", "Surname", "Location", "city", "pin", "doj"]

# Function to check if files exist in the directory
def check_files(**kwargs):
    files = [f for f in os.listdir(DIRECTORY_PATH) if f.endswith(".csv")]
    if not files:
        print("No files in the directory. Flow ends here.")
        return "no_files_task"
    print(f"Files found: {files}")
    kwargs['ti'].xcom_push(key='files', value=files)

# Function to validate and clean records
def validate_records(**kwargs):
    files = kwargs['ti'].xcom_pull(key='files', task_ids='check_files')
    all_valid_records = []

    for file in files:
        file_path = os.path.join(DIRECTORY_PATH, file)
        df = pd.read_csv(file_path)

        # Check schema
        if list(df.columns) != HEADER:
            print(f"File {file} has an invalid schema. Skipping file.")
            continue

        # Validate data types and rules
        valid_records = []
        for _, row in df.iterrows():
            try:
                # Validate emp_id: must be a string (treated as varchar)
                if not isinstance(row["emp_id"], str):
                    raise ValueError("Invalid emp_id")

                # Validate name and surname: must not contain digits
                if re.search(r"\d", row["name"]) or re.search(r"\d", row["Surname"]):
                    raise ValueError("Name or Surname contains digits")

                # Validate pin: must be a 6-digit number
                if not re.match(r"^\d{6}$", str(row["pin"])):
                    raise ValueError("Invalid pin")

                # Validate doj: must be in dd-mm-yyyy format
                row['doj'] = datetime.strptime(row["doj"], "%d-%m-%Y").strftime("%Y-%m-%d")

                # If all validations pass, add to valid records
                valid_records.append(row.to_dict())
            except ValueError as e:
                print(f"Rejected record in file {file}: {row.to_dict()}, Reason: {e}")

        all_valid_records.extend(valid_records)

    if not all_valid_records:
        print("No valid records found across all files.")
        return "no_valid_records_task"

    # Push valid records to XCom for the next task
    kwargs['ti'].xcom_push(key="valid_records", value=all_valid_records)

def create_table_employee_data(**kwargs):
    query ="""
        CREATE TABLE IF NOT EXISTS employee_data (
            id SERIAL PRIMARY KEY,
            emp_id VARCHAR(50),
            name VARCHAR(100),
            surname VARCHAR(100),
            location VARCHAR(100),
            city VARCHAR(100),
            pin CHAR(6),
            doj DATE
        );
        """
    hook = PostgresHook(postgres_conn_id='postgres_default')
    with hook.get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(query)
            print("Employee data table created successfully.")


def check_table_exists(**kwargs):
    query = """
                SELECT EXISTS (
                    SELECT 1
                    FROM information_schema.tables
                    WHERE table_schema = 'public'
                    AND table_name = 'employee_data'
                );
            """
    hook = PostgresHook(postgres_conn_id='postgres_default')
    with hook.get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(query)
            output = cursor.fetchone()
            if output[0]:
                print("Employee Data table exists.")
                return 'table_exists_task'
            print("Employee Data table does not exist.")
            return 'create_table_employee_data_task'
        
# Function to insert valid records into Postgres
def insert_into_postgres(**kwargs):
    valid_records = kwargs['ti'].xcom_pull(key='valid_records', task_ids='validate_records')
    if not valid_records:
        print("No valid records to insert.")
        return

    # Insert into Postgres
    hook = PostgresHook(postgres_conn_id='postgres_default')
    insert_query = """
        INSERT INTO employee_data (emp_id, name, surname, location, city, pin, doj)
        VALUES (%s, %s, %s, %s, %s, %s, %s);
    """

    with hook.get_conn() as conn:
        with conn.cursor() as cursor:
            for record in valid_records:
                cursor.execute(insert_query, (
                    record["emp_id"],
                    record["name"],
                    record["Surname"],
                    record["Location"],
                    record["city"],
                    record["pin"],
                    record["doj"],
                ))
            conn.commit()
            print("Valid records inserted into the Postgres table successfully.")

# Define the DAG
with DAG(
    dag_id="validate_and_insert_csv_to_postgres",
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:
    
    start_task = EmptyOperator(task_id="start_task")

    check_table_task = PythonOperator(
        task_id = 'check_table_task',
        python_callable=check_table_exists,
        provide_context=True,
    )

    table_exists_task = EmptyOperator(task_id='table_exists_task')

    create_table_employee_data_task = PythonOperator(
        task_id='create_table_employee_data_task',
        python_callable=create_table_employee_data,
        provide_context=True,
    )

    check_files_task = PythonOperator(
        task_id="check_files",
        python_callable=check_files,
        provide_context=True,
    )

    no_files_task = EmptyOperator(task_id="no_files_task")

    validate_records_task = PythonOperator(
        task_id="validate_records",
        python_callable=validate_records,
        provide_context=True,
    )

    no_valid_records_task = EmptyOperator(task_id="no_valid_records_task")

    insert_into_postgres_task = PythonOperator(
        task_id="insert_into_postgres",
        python_callable=insert_into_postgres,
        provide_context=True,
    )

    end_task = EmptyOperator(task_id='end_task')

    # Define task dependencies
    start_task >> check_table_task 
    check_table_task >> [table_exists_task, create_table_employee_data_task]
    create_table_employee_data_task >> check_files_task 
    check_files_task >> [no_files_task, validate_records_task]
    validate_records_task >> [no_valid_records_task, insert_into_postgres_task]
    insert_into_postgres_task >> end_task