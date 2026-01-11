from airflow import DAG
from airflow.operators.python import PythonOperator

def push_xcom(**context):
   context['task_instance'].xcom_push(key='my_key', value='Hello XCom!')

def pull_xcom(**context):
   message = context['task_instance'].xcom_pull(task_ids='push_task', key='my_key')
   print(f"Message from XCom: {message}")

with DAG(dag_id='dag_with_xcom') as dag:

   push_task = PythonOperator(
       task_id='push_task',
       python_callable=push_xcom,
       provide_context=True
   )

   pull_task = PythonOperator(
       task_id='pull_task',
       python_callable=pull_xcom,
       provide_context=True
   )
   push_task >> pull_task
