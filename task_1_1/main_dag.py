import  pandas as pd
import os
from airflow import DAG
from datetime import datetime
from airflow.utils.task_group import TaskGroup
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.bash import BashOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


def load_balance(table_name):
    log_start(table_name)

    dag_path = os.path.dirname(os.path.realpath(__file__))
    path=os.path.join(dag_path, 'files_project', f'{table_name}.csv')

    df = pd.read_csv(path, sep=';')
    df['ON_DATE']=pd.to_datetime(df['ON_DATE'], format='%d.%m.%Y')
    upsert_query(table_name, 'pk_balance', df)
    log_end(table_name)
def load_posting(table_name):
    log_start(table_name)
    pg_hook = PostgresHook(postgres_conn_id='postgres-conn')
    engine=pg_hook.get_sqlalchemy_engine()
    dag_path = os.path.dirname(os.path.realpath(__file__))
    path=os.path.join(dag_path, 'files_project', f'{table_name}.csv')

    df = pd.read_csv(path, sep=';')
    df['OPER_DATE']=pd.to_datetime(df['OPER_DATE'], format='%d-%m-%Y')
    df.to_sql(table_name, engine, schema="ds", if_exists="append", index=False)
    log_end(table_name)
def upsert_exchange(table_name):
    log_start(table_name)
    dag_path = os.path.dirname(os.path.realpath(__file__))
    path = os.path.join(dag_path, 'files_project', f'{table_name}.csv')

    df = pd.read_csv(path, sep=';')
    df['DATA_ACTUAL_DATE'] = pd.to_datetime(df['DATA_ACTUAL_DATE'], format='%Y-%m-%d')
    df['DATA_ACTUAL_END_DATE'] = pd.to_datetime(df['DATA_ACTUAL_END_DATE'], format='%Y-%m-%d')
    upsert_query(table_name, 'pk_exchange', df)
    log_end(table_name)

def upsert_query(table_name, constraint, df):
    pg_hook = PostgresHook(postgres_conn_id='postgres-conn')
    engine = pg_hook.get_sqlalchemy_engine()
    upsert_query = """
    INSERT INTO ds.{table_name} ({columns})
    VALUES ({values})
    ON CONFLICT ON CONSTRAINT {constraint}
    DO UPDATE SET {updates};
    """.format(
        table_name=table_name,
        columns=', '.join(df.columns),
        values=', '.join(['%s' for _ in df.columns]),
        constraint=constraint,
        updates=', '.join([f'{col} = EXCLUDED.{col}' for col in df.columns])
    )
    with engine.connect() as conn:
        for row in df.itertuples(index=False, name=None):
            conn.execute(upsert_query, row)

def upsert_ledger_account(table_name):
    log_start(table_name)
    dag_path = os.path.dirname(os.path.realpath(__file__))
    path = os.path.join(dag_path, 'files_project', f'{table_name}.csv')

    df = pd.read_csv(path, sep=';')
    df['START_DATE'] = pd.to_datetime(df['START_DATE'], format='%Y-%m-%d')
    df['END_DATE'] = pd.to_datetime(df['END_DATE'], format='%Y-%m-%d')
    upsert_query(table_name, 'pk_ledger_account', df)
    log_end(table_name)

def load_account(table_name):
    log_start(table_name)

    dag_path = os.path.dirname(os.path.realpath(__file__))
    path=os.path.join(dag_path, 'files_project', f'{table_name}.csv')

    df = pd.read_csv(path, sep=';')
    upsert_query(table_name, 'pk_account', df)
    log_end(table_name)
def load_currency(table_name):
    log_start(table_name)

    dag_path = os.path.dirname(os.path.realpath(__file__))
    path=os.path.join(dag_path, 'files_project', f'{table_name}.csv')

    df = pd.read_csv(path, sep=';', encoding='cp1252')
    upsert_query(table_name, 'pk_currency', df)
    log_end(table_name)
def log_start(table_name):
    pg_hook = PostgresHook(postgres_conn_id='postgres-conn')
    rows = [(f'Upload to {table_name} has started', datetime.now()),]
    target_fields=['log_info', 'log_date_time']
    pg_hook.insert_rows(table='LOGS.logs', rows=rows, target_fields=target_fields)

def log_end(table_name):
    pg_hook = PostgresHook(postgres_conn_id='postgres-conn')
    rows = [(f'Upload to {table_name} successfully completed', datetime.now()), ]
    target_fields = ['log_info', 'log_date_time']
    pg_hook.insert_rows(table='LOGS.logs', rows=rows, target_fields=target_fields)

with DAG(
        dag_id='upload_data_from_csv',
        start_date=datetime(2022, 12, 1),
        catchup=False,
        tags=['neoflex_project']
) as dag:
    start = DummyOperator(
        task_id="start"
    )

    create_table=SQLExecuteQueryOperator(
        task_id='create_tables',
        conn_id='postgres-conn',
        sql='creation_project.sql'
    )
    sleep = BashOperator(
        task_id='sleep',
        bash_command="sleep 5",
    )
    with TaskGroup('balance') as balance:
        load_balance = PythonOperator(
            task_id='load_balance',
            python_callable=load_balance,
            op_kwargs={"table_name":"ft_balance_f"}
        )
    with TaskGroup('posting') as posting:
        load_posting = PythonOperator(
            task_id='load_posting',
            python_callable=load_posting,
            op_kwargs={"table_name": "ft_posting_f"}
        )
    with TaskGroup('account') as account:
        load_account= PythonOperator(
            task_id='load_account',
            python_callable=load_account,
            op_kwargs={"table_name": "md_account_d"}
        )
    with TaskGroup('currency') as currency:
        load_currency = PythonOperator(
            task_id='load_currency',
            python_callable=load_currency,
            op_kwargs={"table_name": "md_currency_d"}
        )
    with TaskGroup('exchange_rate') as exchange_rate:
        load_exchange_rate = PythonOperator(
            task_id='load_exchange_rate',
            python_callable=upsert_exchange,
            op_kwargs={"table_name": "md_exchange_rate_d"}
        )
    with TaskGroup('ledger_account') as ledger_account:
        load_ledger_account = PythonOperator(
            task_id='load_ledger_account_rate',
            python_callable=upsert_ledger_account,
            op_kwargs={"table_name": "md_ledger_account_s"}
        )

    end = DummyOperator(
        task_id="end"
    )

    start >> create_table >> sleep >> [balance, posting,
                                                    account, currency,
                                                    exchange_rate, ledger_account] >> end
