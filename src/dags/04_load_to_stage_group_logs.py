from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator
from vertica_python import connect
import pandas as pd
import pendulum

def load_group_log():
    conn_info = {
        'host': 'vertica.data-engineer.education-services.ru',
        'port': 5433,
        'user': 'vt2603040ca38d',
        'password': '13c9bf25668442e7b77cdcbd4eca0371',
        'database': 'dwh',
        'autocommit': True
    }
    
    df = pd.read_csv('/data/group_log.csv')
    df['user_id_from'] = pd.array(df['user_id_from'], dtype="Int64")
    df.to_csv('/data/group_log_v2.csv', index=False, header=False)
    
    with connect(**conn_info) as conn:
        cur = conn.cursor()
        cur.execute("TRUNCATE TABLE VT2603040CA38D__STAGING.group_log")
        cur.execute("DROP TABLE IF EXISTS VT2603040CA38D__STAGING.group_log_rejected")
        cur.execute("""
            COPY VT2603040CA38D__STAGING.group_log 
            FROM LOCAL '/data/group_log_v2.csv'
            DELIMITER ','
            ENCLOSED BY '"'
            REJECTED DATA AS TABLE VT2603040CA38D__STAGING.group_log_rejected
        """)

with DAG(
    dag_id='04_load_group_log_to_staging',
    schedule_interval=None,
    start_date=pendulum.parse('2022-07-13'),
    catchup=False
) as dag:
    
    start = DummyOperator(task_id='start')
    load = PythonOperator(task_id='load_group_log', python_callable=load_group_log)
    end = DummyOperator(task_id='end')
    
    start >> load >> end