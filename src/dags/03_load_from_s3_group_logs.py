from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.base_hook import BaseHook
import boto3
import pendulum
import os

def load_group_log_from_s3():

    AWS_ACCESS_KEY_ID = "YCAJEiyNFq4wiOe_eMCMCXmQP"
    AWS_SECRET_ACCESS_KEY = "YCP1e96y4QI8OmcB4Eaf4q0nMHwhmtvGbDTgBeqS"
    
    os.makedirs('/data', exist_ok=True)
    
    session = boto3.session.Session()
    s3_client = session.client(
        service_name='s3',
        endpoint_url='https://storage.yandexcloud.net',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )
    
    s3_client.download_file(
        Bucket='sprint6',
        Key='group_log.csv',
        Filename='/data/group_log.csv'
    )
    
with DAG(
    dag_id='03_load_group_log_from_s3',
    schedule_interval=None,
    start_date=pendulum.parse('2022-07-13'),
    catchup=False,
    tags=['s3', 'group_log', 'sprint6']
) as dag:
    
    start = DummyOperator(task_id='start')
    
    load_group_log = PythonOperator(
        task_id='load_group_log',
        python_callable=load_group_log_from_s3
    )
    
    end = DummyOperator(task_id='end')
    
    start >> load_group_log >> end