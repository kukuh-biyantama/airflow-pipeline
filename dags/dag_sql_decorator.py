from airflow.decorators import dag, task
from airflow.sensors.sql import SqlSensor
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
#config
@dag(
    dag_id="example_sql_sensor_decorator",
    start_date=datetime(2024, 8, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["sql", "sensor", "decorator"]
)
def sql_sensor_with_decorator():
    
    # 1️⃣ Sensor (tetap operator klasik)
    wait_for_data = SqlSensor(
        task_id="wait_for_data",
        conn_id="dibimbingconn",
        sql="SELECT 1 FROM bookstore.dim_book LIMIT 1",
        poke_interval=60,
        timeout=1800
    )

    # 2️⃣ Task Python dengan decorator
    @task
    def log_data():
        hook = PostgresHook(postgres_conn_id="dibimbingconn")
        records = hook.get_records(
            "SELECT * FROM bookstore.dim_book LIMIT 5"
        )

        for row in records:
            print(row)

    # dependency
    wait_for_data >> log_data()

sql_sensor_with_decorator()
