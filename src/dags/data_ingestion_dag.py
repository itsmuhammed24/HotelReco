from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from csv_to_raw import convert_csv_to_raw
from raw_to_fmt_imdb import convert_raw_to_formatted
from combine_data import combine_data
from index import index_to_elasticsearch


default_args = {
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'data_ingestion_dag',
    default_args=default_args,
    description='Data Ingestion DAG',
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['example'],
)

sources = ['Hotel_details.csv', 'hotels_RoomPrice.csv']

# Task 0: CSV to Raw
csv_to_raw_tasks = []
for source in sources:
    task_id = f'csv_to_raw_{source.replace(".csv", "")}_task'
    csv_to_raw_task = PythonOperator(
        task_id=task_id,
        python_callable=convert_csv_to_raw,
        op_kwargs={'source': source},
        dag=dag
    )
    csv_to_raw_tasks.append(csv_to_raw_task)

# Task 1: Raw to Formatted
raw_to_formatted_tasks = []
for source in sources:
    task_id = f'raw_to_formatted_{source.replace(".csv", "")}_task'
    raw_to_formatted_task = PythonOperator(
        task_id=task_id,
        python_callable=convert_raw_to_formatted,
        op_kwargs={'source': source},
        dag=dag
    )
    raw_to_formatted_tasks.append(raw_to_formatted_task)

# Task 2: Combine Data
combine_data_task = PythonOperator(
    task_id='combine_data_task',
    python_callable=combine_data,
    dag=dag
)

# Task 3: Index to Elasticsearch
index_to_elasticsearch_task = PythonOperator(
    task_id='index_to_elasticsearch_task',
    python_callable=index_to_elasticsearch,
    dag=dag
)

# Define the task dependencies
for i in range(len(sources)):
    csv_to_raw_tasks[i] >> raw_to_formatted_tasks[i] >> combine_data_task

combine_data_task >> index_to_elasticsearch_task
