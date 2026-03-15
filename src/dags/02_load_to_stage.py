from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.base_hook import BaseHook
from vertica_python import connect
import pendulum

def load_table(table_name, file_name, schema):

    conn = BaseHook.get_connection('vertica_con')
    
    conn_info = {
        'host': conn.host,
        'port': conn.port,
        'user': conn.login,
        'password': conn.password,
        'database': conn.schema or 'dwh',
        'autocommit': True
    }
    
    with connect(**conn_info) as vertica_conn:
        cur = vertica_conn.cursor()
        
        cur.execute(f"TRUNCATE TABLE {schema}.{table_name}")    
        cur.execute(f"DROP TABLE IF EXISTS {schema}.{table_name}_rejected")
        
        cur.execute(f"""
            COPY {schema}.{table_name} 
            FROM LOCAL '/data/{file_name}'
            DELIMITER ','
            ENCLOSED BY '"'
            SKIP 1
            REJECTED DATA AS TABLE {schema}.{table_name}_rejected
        """)
        
        vertica_conn.commit()

with DAG(
    dag_id='02_load_to_stage',
    schedule_interval=None,
    start_date=pendulum.parse('2022-07-13'),
    catchup=False,
    tags=['staging', 'sprint6']
) as dag:
    
    start = DummyOperator(task_id='start')
    
    load_users = PythonOperator(
        task_id='load_users',
        python_callable=load_table,
        op_kwargs={
            'table_name': 'users',
            'file_name': 'users.csv',
            'schema': 'VT2603040CA38D__STAGING'
        }
    )
    
    load_groups = PythonOperator(
        task_id='load_groups',
        python_callable=load_table,
        op_kwargs={
            'table_name': 'groups',
            'file_name': 'groups.csv',
            'schema': 'VT2603040CA38D__STAGING'
        }
    )
    
    load_dialogs = PythonOperator(
        task_id='load_dialogs',
        python_callable=load_table,
        op_kwargs={
            'table_name': 'dialogs',
            'file_name': 'dialogs.csv',
            'schema': 'VT2603040CA38D__STAGING'
        }
    )
    
    end = DummyOperator(task_id='end')
    
    start >> [load_users, load_groups, load_dialogs] >> end