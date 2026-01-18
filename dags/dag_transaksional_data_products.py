from airflow.decorators import dag, task
from airflow.sensors.filesystem import FileSensor
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from datetime import datetime
import pandas as pd
#Dag with Variable
CSV_PATH = Variable.get("csv_products_path")
#Dag with full config
@dag(
   dag_id            = "dag_transaksional_data_products",
   description       = "this for DAG description",
   schedule_interval=None,
   start_date=datetime(2024, 8, 1),
   catchup           = False,
   tags              = ["data engineering", "transaksional", "data pipeline"],
   default_args      = {
       "owner": "Kukuh Biyantama",
   },
)

#Dag Transaksional ETL Pipeline Decorator
def elt_pipeline():
    #sensor
    wait_for_csv = FileSensor(
        task_id="wait_for_csv",
        filepath=CSV_PATH,
        poke_interval=30,
        timeout=600
    )
    #task 1
    @task
    def create_table_if_not_exists():
        hook = PostgresHook(postgres_conn_id="dibimbingconn")

        exists = hook.get_first("""
            SELECT EXISTS (
                SELECT 1
                FROM information_schema.tables
                WHERE table_name = 'raw_products'
            );
        """)[0]

        if exists:
            print("Table sudah ada")
            return "SKIP"

        hook.run("""
            CREATE TABLE raw_products (
                idx INTEGER,
                name TEXT,
                description TEXT,
                brand TEXT,
                category TEXT,
                price NUMERIC,
                currency TEXT,
                stock INTEGER,
                ean TEXT,
                color TEXT,
                size TEXT,
                availability TEXT,
                internal_id INTEGER
            );
        """)
        return "CREATED"

    #task 2
    @task
    def load_csv():
        hook = PostgresHook(postgres_conn_id="dibimbingconn")
        engine = hook.get_sqlalchemy_engine()

        total_rows = 0

        for chunk in pd.read_csv(CSV_PATH, chunksize=1000):
            #normalize columns
            chunk.columns = (
                chunk.columns
                .str.strip()
                .str.lower()
                .str.replace(" ", "_")
            )

            # rename
            chunk = chunk.rename(columns={
                "index": "idx",
                "internal_id": "internal_id"
            })

            chunk.to_sql(
                name="raw_products",
                con=engine,
                if_exists="append",
                index=False
            )

            total_rows += len(chunk)

        return total_rows


    #task 3
    @task
    def transform(rows):
        print(f"Transform {rows} rows")
        hook = PostgresHook(postgres_conn_id="dibimbingconn")
        hook.run("""
            CREATE TABLE IF NOT EXISTS dim_products AS
            SELECT
                name, brand, category, price, stock
            FROM raw_products;
        """)
    create = create_table_if_not_exists()
    load = load_csv()
    trans = transform(load)

    wait_for_csv >> create >> load >> trans

elt_pipeline()
