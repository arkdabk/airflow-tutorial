import datetime
import os
import logging
 
from airflow import models
# from airflow.contrib.operators import bigquery_to_gcs
# from airflow.contrib.operators import gcs_to_bq
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators import gcs_to_gcs
 
#from airflow.utils import trigger_rule
 
yesterday = datetime.datetime.combine(
    datetime.datetime.today() - datetime.timedelta(1),
    datetime.datetime.min.time())
 
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
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
    #'project_id': models.Variable.get('gcp_project')
}
 
bash_cmd='ssh ardab@dataocean-client01.gazeta.pl "ls /tmp/"'
bash_remote_move_logs_to_GCP='ssh ardab@dataocean-client01.gazeta.pl "gsutil -m mv /logi_hive/* gs://b-logs/hive-logs-new"'
with models.DAG(
    'bash_remote_machine_first_dag',
    # Continue to run DAG once per day
    schedule_interval="@once",
    default_args=default_dag_args) as dag:
        start = DummyOperator(task_id='start')   
        end = DummyOperator(task_id='end')
        bash_remote_machine = BashOperator(task_id='bash_remote_machine_task',bash_command=bash_cmd)
        bash_remote_move_logs_to_GCP = BashOperator(task_id='bash_move_logs_to_GCP',bash_command=bash_remote_move_logs_to_GCP)
     
start >> bash_remote_machine >> bash_remote_move_logs_to_GCP >> end
