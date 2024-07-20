from airflow import DAG
from datetime import datetime
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


with DAG(
        dag_id='create_Data_Marts',
        start_date=datetime(2022, 12, 1),
        catchup=False,
        tags=['neoflex_project']
) as dag:
    start = DummyOperator(
        task_id="start"
    )
    task_1_2 = SQLExecuteQueryOperator(
        task_id='calculate_balance_and_turnover_marts',
        conn_id='postgres-conn',
        sql='1.2_script.sql'
    )
    task_1_3 = SQLExecuteQueryOperator(
        task_id='calculate_f101_round',
        conn_id='postgres-conn',
        sql='1.3_script.sql'
    )
    end = DummyOperator(
        task_id="end"
    )
    start >> task_1_2 >> task_1_3 >> end
