from airflow import DAG
from airflow.models import Variable
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator

from datetime import datetime, timedelta

from libs.s3_to_postgres import process_table_files

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    'start_date': days_ago(1),
    "retry_delay": timedelta(minutes=2),
}

S3_TABLES = Variable.get("s3_to_pg_tables", default_var="", deserialize_json=True)
S3_BUCKET = 'systech'

def process_table(table_name, **kwargs):
    params = kwargs["params"]
    process_table_files(S3_BUCKET, table_name, params.get("date_start"), params.get("date_end"))

with DAG(
    dag_id='transfer_systech_to_ekf',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    params={
        'date_start': None, 
        'date_end': None
    }
) as dag:
    
    for table_name in S3_TABLES:
        PythonOperator(
            task_id=f"transfer_{table_name}",
            python_callable=process_table,
            op_kwargs={"table_name": table_name}
        )