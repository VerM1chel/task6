from data_preparator import DataPreparator
from snowflake_creator import SnowflakeCreater
from create_item import ItemCreator

from datetime import datetime

import pandas as pd
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeHook, SnowflakeOperator


def write_from_csv_to_raw():
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    alch = hook.get_sqlalchemy_engine()

    df = pd.read_csv(Variable.get('ios_apps_csv_path'))
    df.to_sql('raw_table', con=alch, if_exists='append', index=False, chunksize=10000)


with DAG('snowflake_task',
         schedule_interval='@once',
         start_date=datetime(2022, 8, 26, 0, 0),

         catchup=False
         ) as dag:
    data_preparator = DataPreparator(
        task_id="csv_parser",
        input_file_name='/home/vermichel/Coding/playroom/task6/763K_plus_IOS_Apps_Info.csv',
        output_file_directory='/home/vermichel/Coding/playroom/task6/cleaned_data/'
    )
    tables_streams_stages_create = ItemCreator(
        task_id='recreate_item',
        account="BD75945.eu-north-1.aws",
        database="TASK6",
        warehouse="COMPUTE_WH",
        conn_id='snowflake_connector',
        activate=False
    )
    write_from_csv_to_raw_task = PythonOperator(
        task_id='write_from_csv_to_raw',
        python_callable=write_from_csv_to_raw
    )

    write_from_raw_to_stage_task = SnowflakeOperator(
        task_id='write_from_raw_to_stage',
        snowflake_conn_id='snowflake_conn',
        sql='write_from_raw_to_stage.sql'
    )

    write_from_stage_to_master_task = SnowflakeOperator(
        task_id='write_from_stage_to_master',
        snowflake_conn_id='snowflake_conn',
        sql='write_from_stage_to_master.sql'
    )

    data_preparator >> tables_streams_stages_create >> write_from_csv_to_raw_task >> write_from_raw_to_stage_task >> write_from_stage_to_master_task