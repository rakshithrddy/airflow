from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator

# Define the Python functions
def sample_python_function(**kwargs):
    print("Running the Python Operator!")

def branching_function(**kwargs):
    # Determine the branch based on a condition
    condition = kwargs.get('condition', 'go_to_task_1')
    if condition == 'go_to_task_1':
        return 'task_1'
    return 'task_2'

def task_1_action():
    print("Task 1 executed.")

def task_2_action():
    print("Task 2 executed.")

# Define the DAG
with DAG(
    dag_id="branching_dag_example",
    schedule="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:
    
    # Python Operator
    python_task = PythonOperator(
        task_id="python_task",
        python_callable=sample_python_function,
    )
    
    # BranchPython Operator
    branch_task = BranchPythonOperator(
        task_id="branching_task",
        python_callable=branching_function,
        op_kwargs={'condition': 'go_to_task_2'},  # Change this to 'go_to_task_2' to branch differently
    )
    
    # Dummy Operator for branching
    task_1 = PythonOperator(
        task_id="task_1",
        python_callable=task_1_action,
    )
    
    task_2 = PythonOperator(
        task_id="task_2",
        python_callable=task_2_action,
    )
    
    join_task = EmptyOperator(
        task_id="join_task",
        trigger_rule="none_failed_min_one_success",  # Ensures all paths converge
    )
    
    # Define the workflow
    python_task >> branch_task
    branch_task >> task_1 >> join_task
    branch_task >> task_2 >> join_task
