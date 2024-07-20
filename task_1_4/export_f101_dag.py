from airflow import DAG
import os
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
import pandas as pd
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime

def export_to_csv():
    pg_hook = PostgresHook(postgres_conn_id='postgres-conn')
    dag_path = os.path.dirname(os.path.realpath(__file__))
    path=os.path.join(dag_path, 'files_project', 'dm_f101_round_f.csv')
    df = pg_hook.get_pandas_df(sql="SELECT * FROM dm.dm_f101_round_f")

    df.to_csv(path, index=False)
def log_start():
    pg_hook = PostgresHook(postgres_conn_id='postgres-conn')
    rows = [('Export table dm _f101_round_f to csv-file has started', datetime.now()),]
    target_fields=['log_info', 'log_date_time']
    pg_hook.insert_rows(table='LOGS.logs', rows=rows, target_fields=target_fields)

def log_end():
    pg_hook = PostgresHook(postgres_conn_id='postgres-conn')
    rows = [('Export table dm _f101_round_f has completed successfully', datetime.now()), ]
    target_fields = ['log_info', 'log_date_time']
    pg_hook.insert_rows(table='LOGS.logs', rows=rows, target_fields=target_fields)

with DAG(dag_id='export_to_csv',
        start_date=datetime(2022, 12, 1),
        catchup=False,
        tags=['neoflex_project']) as dag:
    export = PythonOperator(
        task_id='export_to_csv',
        python_callable=export_to_csv
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
    start >> log_start>>export>>log_end >> end


