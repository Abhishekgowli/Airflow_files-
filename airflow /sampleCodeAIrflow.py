from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
from datetime import datetime, timedelta


# Define a simple Python function for the PythonOperator
def hello_world_py(*args, **kwargs):
    print('Hello World from PythonOperator')

default_args={
    'owner': 'airflow',
    'depends_on_past': False,
    # 'email_on_failure': False,
    # 'email_on_retry': False,
    # 'email': ['your-email@example.com'],
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}


# Define the DAG
dag = DAG(
    'first_dag_for_seq_tasks',
    default_args=default_args,
    description='A dummy DAG',
    start_date=datetime(2024, 12, 15),
    schedule_interval="*/1 * * * *",
    catchup=False,
    tags=['dev'],
)

    # Task 1: Run a simple Bash command
bash_task = BashOperator(
        task_id="bash_hello",
        bash_command="echo 'Hello from Bash!'",
        dag=dag
    )

    # Task 2: Run a Python function
python_task = PythonOperator(
        task_id="python_greet",
        python_callable=hello_world_py,
        dag=dag,
    )

    # Define task dependencies
bash_task >> python_task
