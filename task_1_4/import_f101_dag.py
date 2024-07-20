from airflow import DAG
import os
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
import pandas as pd
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

def upload_to_bd():
    pg_hook = PostgresHook(postgres_conn_id='postgres-conn')
    engine = pg_hook.get_sqlalchemy_engine()
    dag_path = os.path.dirname(os.path.realpath(__file__))
    path=os.path.join(dag_path, 'files_project', 'dm_f101_round_f.csv')
    df = pd.read_csv(path)
    df.to_sql('dm_f101_round_f_v2', engine, schema="dm", if_exists="append", index=False)
def log_start():
    pg_hook = PostgresHook(postgres_conn_id='postgres-conn')
    rows = [('Upload to copy of table dm_f101_round_f has started', datetime.now()),]
    target_fields=['log_info', 'log_date_time']
    pg_hook.insert_rows(table='LOGS.logs', rows=rows, target_fields=target_fields)

def log_end():
    pg_hook = PostgresHook(postgres_conn_id='postgres-conn')
    rows = [('Upload to copy of table dm_f101_round_f has completed successfully', datetime.now()), ]
    target_fields = ['log_info', 'log_date_time']
    pg_hook.insert_rows(table='LOGS.logs', rows=rows, target_fields=target_fields)

with DAG(dag_id='upload_copy',
        start_date=datetime(2022, 12, 1),
        catchup=False,
        tags=['neoflex_project']) as dag:
    upload = PythonOperator(
        task_id='export_to_csv',
        python_callable=upload_to_bd
    )
    create_table = SQLExecuteQueryOperator(
        task_id='create_tables',
        conn_id='postgres-conn',
        sql='CREATE TABLE if not exists dm.dm_f101_round_f_v2 AS TABLE dm.dm_f101_round_f WITH NO DATA;'
    )
    start = DummyOperator(
        task_id="start"
    )
    end = DummyOperator(
        task_id="end"
    )
    log_start=PythonOperator(
        task_id='start_log',
        python_callable=log_start)
    log_end=PythonOperator(
        task_id='end_log',
        python_callable=log_end)
    start >> log_start >> create_table >> upload >> log_end >> end
