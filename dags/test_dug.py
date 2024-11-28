from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.decorators import task

from datetime import datetime, timedelta
import logging

from libs.s3.s3_handler import S3Handler
from libs.postgresql.postgresql import get_connection_postgres

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id='test_dag',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    max_active_runs=1, 
) as dag:
    
    @task
    def test_s3_conn():
        s3 = S3Handler()
        logging.info(s3)
    
    @task
    def test_pg_conn():
        engine =  get_connection_postgres()
        logging.info(engine)

    

    test_s3_conn()
    test_pg_conn()  
