import pandas
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from datetime import datetime
from airflow.configuration import conf
from airflow.models import Variable
import time


PATH = Variable.get("my_path")
conf.set("core", "template_searchpath", PATH)


def log_to_db(start_time, end_time, duration, table_name, message):
    postgres_hook = PostgresHook("postgres_db")
    engine = postgres_hook.get_sqlalchemy_engine()

    with engine.begin() as conn:
        conn.execute(
            f"""
            INSERT INTO logs.data_load_logs (start_time, end_time, duration, table_name, message)
            VALUES ('{start_time}', '{end_time}',
                    '{duration}', '{table_name}', '{message}')
            """
        )


def insert_data(table_name, encoding='utf-8', delimiter=';', dtype=None, parse_dates=None, dayfirst=False):
    start_time = datetime.now()
    time.sleep(5)

    df = pandas.read_csv(PATH + f"{table_name}.csv", delimiter=delimiter,
                         encoding=encoding, dtype=dtype, parse_dates=parse_dates, dayfirst=dayfirst)
    df.columns = df.columns.str.lower()
    df = df.drop_duplicates()
    df = df.where(pandas.notnull(df), None)

    postgres_hook = PostgresHook("postgres_db")
    engine = postgres_hook.get_sqlalchemy_engine()
    primary_keys = {
        "ft_balance_f": ["on_date", "account_rk"],
        "md_account_d": ["data_actual_date", "account_rk"],
        "md_currency_d": ["currency_rk", "data_actual_date"],
        "md_exchange_rate_d": ["data_actual_date", "currency_rk"],
        "md_ledger_account_s": ["ledger_account", "start_date"]
    }

    with engine.begin() as conn:
        if table_name == "ft_posting_f":
            conn.execute("TRUNCATE TABLE ds.ft_posting_f;")
            for _, row in df.iterrows():
                insert_query = f"""
                INSERT INTO "ds".{table_name} ({', '.join([col for col in row.index])})
                VALUES ({', '.join(['%s'] * len(row))})
                """
                conn.execute(insert_query, tuple(row))
        if table_name in primary_keys:
            pk_columns = primary_keys.get(table_name)
            for _, row in df.iterrows():
                insert_query = f"""
                INSERT INTO "ds".{table_name} ({', '.join([col for col in row.index])})
                VALUES ({', '.join(['%s'] * len(row))})
                ON CONFLICT ({', '.join(pk_columns)})
                DO UPDATE SET {', '.join([f"{col} = EXCLUDED.{col}" for col in row.index if col not in pk_columns])}
                RETURNING *;
                """
                conn.execute(insert_query, tuple(row))

    end_time = datetime.now()
    duration = end_time - start_time
    message = f"""Загружено {len(df)} строк в таблицу {table_name}. Длительность: {
        duration.total_seconds()} секунд"""
    log_to_db(start_time, end_time, duration, table_name, message)


default_args = {
    "owner": "abershits",
    "start_date": datetime.now(),
    "retries": 2
}

with DAG(
    "insert_data",
    default_args=default_args,
    description="Загрузка данных в DS",
    catchup=False,
    template_searchpath=[PATH],
    schedule="0 0 * * *"
) as dag:

    start = EmptyOperator(
        task_id="start"
    )

    ft_balance_f = PythonOperator(
        task_id="ft_balance_f",
        python_callable=insert_data,
        op_kwargs={
            "table_name": "ft_balance_f",
            "delimiter": ',',
            "parse_dates": ["ON_DATE"],
            "dayfirst": True
        }
    )

    ft_posting_f = PythonOperator(
        task_id="ft_posting_f",
        python_callable=insert_data,
        op_kwargs={
            "table_name": "ft_posting_f",
            "parse_dates": ["OPER_DATE"],
            "dayfirst": True
        }
    )

    md_account_d = PythonOperator(
        task_id="md_account_d",
        python_callable=insert_data,
        op_kwargs={
            "table_name": "md_account_d",
            "parse_dates": ["DATA_ACTUAL_DATE", "DATA_ACTUAL_END_DATE"]
        }
    )

    md_currency_d = PythonOperator(
        task_id="md_currency_d",
        python_callable=insert_data,
        op_kwargs={
            "table_name": "md_currency_d",
            "encoding": "Windows-1252",
            "dtype": {"CURRENCY_CODE": str},
            "parse_dates": ["DATA_ACTUAL_DATE", "DATA_ACTUAL_END_DATE"]
        }
    )

    md_exchange_rate_d = PythonOperator(
        task_id="md_exchange_rate_d",
        python_callable=insert_data,
        op_kwargs={
            "table_name": "md_exchange_rate_d",
            "parse_dates": ["DATA_ACTUAL_DATE", "DATA_ACTUAL_END_DATE"]
        }
    )

    split = EmptyOperator(
        task_id="split"
    )

    md_ledger_account_s = PythonOperator(
        task_id="md_ledger_account_s",
        python_callable=insert_data,
        op_kwargs={
            "table_name": "md_ledger_account_s",
            "parse_dates": ["START_DATE", "END_DATE"]
        }
    )

    end = EmptyOperator(
        task_id="end"
    )

    (
        start
        >> [ft_balance_f, ft_posting_f, md_currency_d, md_account_d, md_exchange_rate_d]
        >> split
        >> md_ledger_account_s
        >> end
    )
