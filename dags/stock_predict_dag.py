from airflow import DAG
from airflow.operators.python import BranchPythonOperator
from airflow.operators.dummy import DummyOperator
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

def decide_branch(**kwargs):
    hour = kwargs['logical_date'].hour
    if 1 <= hour <= 12:
        return "run_stock_predict_notebook"
    else:
        return "skip_task"

with DAG(
    dag_id='stock_predict_hourly_papermill',
    default_args=default_args,
    start_date=datetime(2025, 7, 26, 9, 0, tzinfo=local_tz),
    schedule_interval='@hourly',
    catchup=False,
    max_active_runs=1,
    tags=['colab', 'papermill']
) as dag:

    branch = BranchPythonOperator(
        task_id="check_hour_and_branch",
        python_callable=decide_branch,
        provide_context=True,
    )

    run_notebook = PapermillOperator(
        task_id="run_stock_predict_notebook",
        input_nb="/home/airflow/gcs/data/notebooks/in/stock_predict_table.ipynb",
        output_nb="/home/airflow/gcs/data/notebooks/out/stock_predict_table-{{ ds_nodash }}.ipynb",
        parameters={
            "AWS_ACCESS_KEY_ID": Variable.get("AWS_ACCESS_KEY_ID"),
            "AWS_SECRET_ACCESS_KEY": Variable.get("AWS_SECRET_ACCESS_KEY"),
            "EC2_PUBLIC_IP": Variable.get("EC2_PUBLIC_IP"),
            "SNOWFLAKE_ACCOUNT": Variable.get("SNOWFLAKE_ACCOUNT"),
            "SNOWFLAKE_DATABASE": Variable.get("SNOWFLAKE_DATABASE"),
            "SNOWFLAKE_PASSWORD": Variable.get("SNOWFLAKE_PASSWORD"),
            "SNOWFLAKE_SCHEMA": Variable.get("SNOWFLAKE_SCHEMA"),
            "SNOWFLAKE_USER": Variable.get("SNOWFLAKE_USER"),
            "SNOWFLAKE_WAREHOUSE": Variable.get("SNOWFLAKE_WAREHOUSE"),
            "target_date": "{{ ds }}",
            "ref_time_str": "{{  (execution_date + macros.timedelta(hours=1)).replace(minute=0, second=0, microsecond=0).strftime('%H:%M') }}"
        },
    )

    skip_task = DummyOperator(
        task_id="skip_task"
    )

    branch >> [run_notebook, skip_task]