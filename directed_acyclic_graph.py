'''
=================================================
This program is made to connect with airflow with several configurations such as scheduling starting on November 01, 2024 every Saturday 
at 09:10 AM to 09:30 AM and performed per 10 minutes. By invoking bashOperator according to the DAG that has been created.
=================================================
'''

import datetime as dt
from datetime import timedelta

from airflow import DAG
from airflow.operators.bash_operator import BashOperator

import pandas as pd

default_args = {
    'owner': 'agif',
    'start_date': dt.datetime(2024, 11, 1),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=10),
}


with DAG('Milestone3',
         default_args=default_args,
         schedule_interval='10-30/10 9 * * 6',
         catchup=False
         ) as dag:

    extract_transform_financials = BashOperator(task_id='extract_transform_data', bash_command='python /opt/airflow/dags/extract_transform.py')

    load_financials = BashOperator(task_id='load_data', bash_command='python /opt/airflow/dags/load.py')

extract_transform_financials >> load_financials
