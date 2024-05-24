from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago
import papermill as pm

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

# Function to run a notebook using Papermill
def run_notebook(notebook_path, output_path):
    pm.execute_notebook(notebook_path, output_path)

# Define tasks
start = DummyOperator(task_id='start', dag=dag)

run_first_notebook = PythonOperator(
    task_id='run_first_notebook',
    python_callable=run_notebook,
    op_kwargs={
        'notebook_path': '/Users/ronitguptaaa/Documents/MusicETL/Workflow.ipynb'
    },
    dag=dag,
)

# run_second_notebook = PythonOperator(
#     task_id='run_second_notebook',
#     python_callable=run_notebook,
#     op_kwargs={
#         'notebook_path': '/path/to/your/second_notebook.ipynb',
#         'output_path': '/path/to/your/output_second_notebook.ipynb'
#     },
#     dag=dag,
# )

# run_third_notebook = PythonOperator(
#     task_id='run_third_notebook',
#     python_callable=run_notebook,
#     op_kwargs={
#         'notebook_path': '/path/to/your/third_notebook.ipynb',
#         'output_path': '/path/to/your/output_third_notebook.ipynb'
#     },
#     dag=dag,
# )

end = DummyOperator(task_id='end', dag=dag)

# Set task dependencies
start >> run_first_notebook >> end
