from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator
from datetime import datetime

@dag(
   dag_id            = "dag_full_config",
   description       = "this for DAG description",
   schedule_interval = "* * * * *",
   start_date        = datetime(2024, 7, 1),
   catchup           = False,
   tags              = ["tag dag"],
   default_args      = {
       "owner": "dibimbing students",
   },
   owner_links = {
       "dibimbing": "https://dibimbing.id/",
       "students"    : "mailto:owner@dibimbing.id",
   }
)
def main():
   task_1 = EmptyOperator(
       task_id = "first_task"
   )
   task_1
main()