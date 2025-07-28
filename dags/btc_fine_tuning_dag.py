from airflow import DAG
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.operators.python import PythonOperator
from airflow.providers.papermill.operators.papermill import PapermillOperator
from datetime import datetime, timedelta
from airflow.models import Variable

import pendulum
import os

local_tz = pendulum.timezone("Asia/Seoul")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id='btc_fine_tuning_papermill',
    default_args=default_args,
    start_date=datetime(2025, 7, 21, 20, 45, tzinfo=local_tz),
    schedule_interval=timedelta(days=1),
    catchup=True,
    max_active_runs=1,
    tags=['colab', 'papermill']
)


run_notebook = PapermillOperator(
    task_id="run_fine_tuning_notebook",
    input_nb="/home/airflow/gcs/data/notebooks/in/Btc_fine_tuning.ipynb",
    output_nb="/home/airflow/gcs/data/notebooks/out/Btc-fine-tuning-{{ ds_nodash }}.ipynb",
    parameters={
        "AWS_ACCESS_KEY_ID": Variable.get("AWS_ACCESS_KEY_ID"),
        "AWS_SECRET_ACCESS_KEY": Variable.get("AWS_SECRET_ACCESS_KEY"),
        "target_date": "{{  (execution_date + macros.timedelta(days=1)).strftime('%Y-%m-%d')  }}"
    },
    dag=dag,
)

run_notebook
