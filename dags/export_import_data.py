import pandas
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from datetime import datetime
from airflow.configuration import conf
from airflow.models import Variable
import os

PATH = Variable.get('my_path')
conf.set('core', 'template_searchpath', PATH)

def log_to_db(start_time, end_time, duration, table_name, message):
    'Функция для записи логов в базу данных'
    postgres_hook = PostgresHook('postgres_db')
    engine = postgres_hook.get_sqlalchemy_engine()

    with engine.begin() as conn:
        conn.execute(
            f'''
            INSERT INTO logs.data_load_logs(start_time, end_time, duration, table_name, message)
            VALUES('{start_time}', '{end_time}', '{duration}' ,'{table_name}' ,'{message}')
            '''
        )

def export_to_csv(table_name):
    "Функция для выгрузки данных из базы данных в CSV-файл"
    start_time = datetime.now()
    csv_file_path = os.path.join(f'{PATH}{table_name}.csv')

    postgres_hook = PostgresHook('postgres_db')
    engine = postgres_hook.get_sqlalchemy_engine()

    with engine.begin() as conn:
        df = pandas.read_sql(f'SELECT * FROM {table_name}', conn)
        df.to_csv(csv_file_path, index=False, encoding='utf-8', sep=',')

    end_time = datetime.now()
    duration = end_time - start_time
    message = f'Данные успешно экспортированы в {csv_file_path}. Длительность: {duration.total_seconds()} секунд'

    log_to_db(start_time, end_time, duration, table_name, message)

def copy_table_structure(table_name):
    'Функция для создании пустой копии переданной таблицы'
    postgres_hook = PostgresHook('postgres_db')
    engine = postgres_hook.get_sqlalchemy_engine()
    with engine.begin() as conn:
        conn.execute(f'''
            CREATE TABLE IF NOT EXISTS {table_name}_v2 AS
            SELECT *
            FROM {table_name}
            WHERE 1 = 2;
        ''')

def change_csv(file_name, encoding='utf-8', delimiter=','):
    'Функция для замены всех NULL-значений на "0"'
    df = pandas.read_csv(os.path.join(PATH + f'{file_name}.csv'), encoding=encoding, delimiter=delimiter)
    df.fillna(0, inplace=True)
    df.to_csv(os.path.join(PATH + f'{file_name}_v2.csv'), index=False, encoding=encoding, sep=delimiter)

def insert_to_sql(table_name, encoding='utf-8', schema='dm'):
    "Функция для загрузки данных из CSV в базу данных"
    start_time = datetime.now()
    df = pandas.read_csv(PATH + f'dm.{table_name}.csv', encoding=encoding, delimiter=',')
    postgres_hook = PostgresHook('postgres_db')
    engine = postgres_hook.get_sqlalchemy_engine()

    df.to_sql(f'{table_name}', engine, schema=schema, if_exists='append', index=False)
            
    end_time = datetime.now()
    duration = end_time - start_time
    message = f"Загружено {len(df)} строк в таблицу dm.{table_name}. Длительность: {duration.total_seconds()} секунд"
    log_to_db(start_time, end_time, duration, table_name, message)

default_args = {
    'owner': 'avbershits',
    'start_date': datetime.now(),
    'retries': 2
}

with DAG(
    'export_import_data',
    default_args=default_args,
    description='Экспорт данных из 101 формы в csv',
    catchup=False,
    template_searchpath=[PATH],
    schedule='0 0 * * *'
) as dag:
    
    start = EmptyOperator(
        task_id='start'
    )

    export_data = PythonOperator(
        task_id='export_data',
        python_callable=export_to_csv,
        op_kwargs={'table_name': 'dm.dm_f101_round_f'}
    )

    create_copy_table_structure = PythonOperator(
        task_id='create_copy_table_structure',
        python_callable=copy_table_structure,
        op_kwargs={'table_name': 'dm.dm_f101_round_f'}
    )

    change_csv_data = PythonOperator(
        task_id='change_csv',
        python_callable=change_csv,
        op_kwargs={'file_name': 'dm.dm_f101_round_f'}
    )

    insert_data = PythonOperator(
        task_id='insert_data',
        python_callable=insert_to_sql,
        op_kwargs={'table_name': 'dm_f101_round_f_v2', 'schema': 'dm'}
    )

    end = EmptyOperator(
        task_id='end'
    )

    (
        start
        >> export_data
        >> create_copy_table_structure
        >> change_csv_data
        >> insert_data
        >> end
    )