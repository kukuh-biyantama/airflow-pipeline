from datetime import datetime

import numpy as np
import pandas as pd
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

# Define constants
ACTION_TYPES = ["walk", "run", "sit", "sleep"]
PERSON_IDS = range(1, 5)


# Function to generate random data
def generate_random_data():
    num_rows = 10
    data = {
        "action_type": np.random.choice(ACTION_TYPES, num_rows),
        "timestamp": pd.date_range(start="2021-01-01", periods=num_rows, freq="T"),
        "person_id": np.random.choice(PERSON_IDS, num_rows),
    }
    print(data)


# Define the default arguments for the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
}

# Define the DAG
dag = DAG(
    "generate_and_ingest_activity_data",
    default_args=default_args,
    description="Generate random activity data and ingest into PostgreSQL",
    schedule_interval="@once",
    start_date=datetime(2023, 1, 1),
    catchup=False,
)

# Define the tasks
t1 = PythonOperator(
    task_id="generate_random_data_1",
    python_callable=generate_random_data,
    dag=dag,
)

t2 = PythonOperator(
    task_id="generate_random_data_2",
    python_callable=generate_random_data,
    dag=dag,
)

# Set the task dependencies
t1 >> t2
