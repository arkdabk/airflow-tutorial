import datetime as dt
import os
import logging
 
from airflow import models
# from airflow.contrib.operators import bigquery_to_gcs
# from airflow.contrib.operators import gcs_to_bq
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators import gcs_to_gcs
 
#from airflow.utils import trigger_rule
 
yesterday = dt.datetime.combine(
    dt.datetime.today() - dt.timedelta(1),
    dt.datetime.min.time())
 
default_dag_args = {
    # Setting start date as yesterday starts the DAG immediately when it is
    # detected in the Cloud Storage bucket.
    'owner':'ardab',
    'start_date': yesterday,
    # To email on failure or retry set 'email' arg to your email and enable
    # emailing here.
    'email_on_failure': False,
    'email_on_retry': False,
    # If a task fails, retry it once after waiting at least 5 minutes
    'retries': 2,
    'retry_delay': dt.timedelta(minutes=3),
    #'project_id': models.Variable.get('gcp_project')
}
 
bash_remote_move_logs_to_GCP='ssh ardab@dataocean-client01.gazeta.pl "gsutil -m mv /logi_hive/* gs://b-logs/hive-logs-new"'
with models.DAG(
    'bash_remote_move_logs_to_GCP',
    # Continue to run DAG once per day
    # schedule_interval='*/5 * * * *',
    schedule_interval= dt.timedelta(minutes=30),
    default_args=default_dag_args) as dag:
        start = DummyOperator(task_id='start')   
        end = DummyOperator(task_id='end')
        bash_remote_move_logs_to_GCP = BashOperator(task_id='bash_move_logs_to_GCP',bash_command=bash_remote_move_logs_to_GCP)
     
start >> bash_remote_move_logs_to_GCP >> end
