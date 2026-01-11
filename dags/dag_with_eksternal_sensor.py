from airflow import DAG
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
   dag_id='example_external_task_sensor',
   start_date=datetime(2024, 8, 1),
   schedule_interval='@daily',
   catchup=False
) as dag:

   wait_for_task = ExternalTaskSensor(
       task_id='wait_for_task_dag_a',
       external_dag_id='dag_a',
       external_task_id='task_in_dag_a',
       poke_interval=60,
       timeout=1800
   )

   run_after_task = BashOperator(
       task_id='run_after_task',
       bash_command='echo "Task in DAG A is done"'
   )

   wait_for_task >> run_after_task
