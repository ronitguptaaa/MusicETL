from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago
import subprocess

# Define default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

# Define the DAG
dag = DAG(
    'music_etl_dag',
    default_args=default_args,
    description='ETL process for music data using Airflow',
    schedule_interval='@daily',
)

# Function to run a notebook
def run_notebook():
    # Execute the notebook using nbconvert
    subprocess.run(["jupyter", "nbconvert", "--to", "notebook", "--execute", "/Users/ronitguptaaa/Documents/MusicETL/Workflow.ipynb"])

# Define tasks
start = DummyOperator(task_id='start', dag=dag)

run_first_notebook = PythonOperator(
    task_id='run_first_notebook',
    python_callable=run_notebook,
    dag=dag,
)

end = DummyOperator(task_id='end', dag=dag)

# Set task dependencies
start >> run_first_notebook >> end
