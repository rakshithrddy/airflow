from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# Function to initialize dates and push to XCom
def initialize_dates(**kwargs):
    # Calculate dates
    today = datetime.today()
    yesterday = today - timedelta(days=1)
    first_day_of_month = today.replace(day=1)
    first_day_of_year = today.replace(month=1, day=1)

    # Push values to XCom
    ti = kwargs["ti"]
    ti.xcom_push(key='today_date', value=today.strftime('%Y-%m-%d'))
    ti.xcom_push(key='yesterday_date', value=yesterday.strftime('%Y-%m-%d'))
    ti.xcom_push(key='first_day_of_month', value=first_day_of_month.strftime('%Y-%m-%d'))
    ti.xcom_push(key='first_day_of_year', value=first_day_of_year.strftime('%Y-%m-%d'))

    print(f"Dates initialized: Today={today}, Yesterday={yesterday}, "
          f"First Day of Month={first_day_of_month}, First Day of Year={first_day_of_year}")
    
# Function to print the initialized dates from XCom
def print_dates(**kwargs):
    # Pull values from XCom
    ti = kwargs['ti']
    today_date = ti.xcom_pull(key='today_date', task_ids='initialize_dates_task')
    yesterday_date = ti.xcom_pull(key='yesterday_date', task_ids='initialize_dates_task')
    first_day_of_month = ti.xcom_pull(key='first_day_of_month', task_ids='initialize_dates_task')
    first_day_of_year = ti.xcom_pull(key='first_day_of_year', task_ids='initialize_dates_task')

    # Print the pulled values
    print(f"Pulled from XCom: Today={today_date}, Yesterday={yesterday_date}, "
          f"First Day of Month={first_day_of_month}, First Day of Year={first_day_of_year}")

# Define the DAG
with DAG(
    dag_id="initialize_dates_dag",
    schedule_interval=None,  # Trigger manually
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:

    # Task 1: Initialize dates and push to XCom
    initialize_dates_task = PythonOperator(
        task_id="initialize_dates_task",
        python_callable=initialize_dates,
        provide_context=True,
        
    )

    # Task 2: Print the dates pulled from XCom
    print_dates_task = PythonOperator(
        task_id="print_dates_task",
        python_callable=print_dates,
        provide_context=True,
    )

    # Define task dependencies
    initialize_dates_task >> print_dates_task
