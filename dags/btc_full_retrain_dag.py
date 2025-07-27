from airflow import DAG
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.operators.python import PythonOperator
from airflow.providers.papermill.operators.papermill import PapermillOperator
from airflow.models import Variable

from datetime import datetime, timedelta
import pendulum

local_tz = pendulum.timezone("Asia/Seoul")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id='btc_full_retrain_papermill',
    default_args=default_args,
    start_date=datetime(2025, 7, 8, 20, 10, tzinfo=local_tz),
    schedule_interval=timedelta(days=14),
    catchup=True,
    max_active_runs=1,
    tags=['colab', 'papermill']
)

run_notebook = PapermillOperator(
    task_id="run_btc_full_retrain_notebook",
    input_nb="/home/airflow/gcs/data/notebooks/in/Btc_full_retrain.ipynb",
    output_nb="/home/airflow/gcs/data/notebooks/out/Btc-full-retrain-{{ ds_nodash }}.ipynb",
    parameters={
        "AWS_ACCESS_KEY_ID": Variable.get("AWS_ACCESS_KEY_ID"),
        "AWS_SECRET_ACCESS_KEY": Variable.get("AWS_SECRET_ACCESS_KEY"),
        "exec_date_str": "{{ (execution_date + macros.timedelta(days=14)).strftime('%Y-%m-%d') }}"
    },
    dag=dag,
)

run_notebook