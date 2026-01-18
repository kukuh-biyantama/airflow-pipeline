from airflow import DAG
from airflow.operators.empty import EmptyOperator

dag = DAG(dag_id="dag_with_variable1")

task_1 = EmptyOperator(
   task_id = "first_task",
   dag     = dag,
)

task_1
