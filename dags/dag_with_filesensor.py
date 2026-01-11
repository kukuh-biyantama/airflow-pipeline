from airflow import DAG
from airflow.sensors.filesystem import FileSensor
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
   dag_id='example_file_sensor',
   start_date=datetime(2024, 8, 1),
   schedule_interval='@daily',
   catchup=False
) as dag:

   wait_for_file = FileSensor(
       task_id='wait_for_input_file',
       filepath='/opt/airflow/data/input.csv',
       poke_interval=60,
       timeout=3600
   )

   process_file = BashOperator(
       task_id='process_file',
       bash_command='echo "Processing file..."'
   )

   wait_for_file >> process_file
