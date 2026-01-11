from airflow import DAG
from airflow.sensors.sql import SqlSensor
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
   dag_id='example_sql_sensor_version',
   start_date=datetime(2024, 8, 1),
   schedule_interval='@daily',
   catchup=False
) as dag:

   wait_for_db = SqlSensor(
       task_id='wait_for_db_version',
       conn_id='dibimbingconn',
       sql='SELECT * from bookstore.dim_book;',
       poke_interval=60,
       timeout=1800
   )

   log_version = BashOperator(
       task_id='log_db_version',
       bash_command='echo "Database is ready, version query executed."'
   )

   wait_for_db >> log_version