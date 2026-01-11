from airflow.decorators import dag, task
from airflow.sensors.sql import SqlSensor
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.mysql.hooks.mysql import MySqlHook
from datetime import timedelta
from datetime import datetime
import pandas as pd


#task 1
def extract_productsperkategori_from_postgres(**context):
    """
    Extract data product dari PostgreSQL
    dan simpan ke XCom
    """
    postgres_hook = PostgresHook(postgres_conn_id="dibimbingconn")

    sql = """
    SELECT
    category,
    COUNT(*) AS total_products
    FROM raw_products
    GROUP BY category
    ORDER BY total_products DESC;
    """

    df = postgres_hook.get_pandas_df(sql)

    # Convert ke JSON-serializable
    data = df.to_dict(orient="records")

    ti = context["ti"]
    ti.xcom_push(key="raw_products", value=data)
    ti.xcom_push(key="row_count", value=len(df))

    print(f"Extracted {len(df)} rows from PostgreSQL")

#task 2
def create_mysql_table_productperkategori(**context):
    """
    Buat tabel MySQL jika belum ada
    """
    mysql_hook = MySqlHook(mysql_conn_id="local_mysql")

    create_table_sql = """
    CREATE TABLE IF NOT EXISTS Jumlah_produk_per_kategori (
        product_id INT PRIMARY KEY,
        category text,
        total_products INT,
        created_at DATETIME
    );
    """

    mysql_hook.run(create_table_sql)
    print("MySQL table checked/created successfully")

#task 3
def transform_productsperkategori(**context):
    """
    Transform data product
    """
    ti = context["ti"]

    raw_data = ti.xcom_pull(
        task_ids="extract_products",
        key="raw_products"
    )

    df = pd.DataFrame(raw_data)

    df["category"] = df["category"].str.lower()
    df["total_products"] = df["total_products"].astype(int)

    transformed_data = df.to_dict(orient="records")

    ti.xcom_push(
        key="transformed_products",
        value=transformed_data
    )

    print(f"Transformed {len(df)} rows")


#task 4
def load_products_to_mysql(**context):
    """
    Load transformed data ke MySQL
    """
    ti = context["ti"]

    data = ti.xcom_pull(
        task_ids="transform_products",
        key="transformed_products"
    )

    df = pd.DataFrame(data)

    mysql_hook = MySqlHook(mysql_conn_id="local_mysql")
    engine = mysql_hook.get_sqlalchemy_engine()

    df.to_sql(
        name="Jumlah_produk_per_kategori",
        con=engine,
        if_exists="append",
        index=False,
        chunksize=1000
    )

    print(f"Loaded {len(df)} rows into MySQL")


#Dag with full config
@dag(
   dag_id            = "dag_ETL_warehouse_postgres_to_mysql",
   description       = "this for DAG description",
   schedule_interval=None,
   start_date=datetime(2024, 8, 1),
   catchup           = False,
   tags              = ["data engineering", "transaksional", "data pipeline"],
   default_args      = {
       "owner": "data-engineering-team",
   },
)

def etl_postgres_to_mysql_pipeline():
    #sensor 
    wait_for_db = SqlSensor(
       task_id='wait_for_db_version',
       conn_id='dibimbingconn',
       sql='SELECT * from raw_products;',
       poke_interval=60,
       timeout=1800
    )


    extract_task_product = task(
        task_id="extract_products",
        python_callable=extract_productsperkategori_from_postgres,
        provide_context=True
    )()

    create_table_task_product = task(
        task_id="create_mysql_table",
        python_callable=create_mysql_table_productperkategori,
        provide_context=True
    )()

    transform_task_product = task(
        task_id="transform_products",
        python_callable=transform_productsperkategori,
        provide_context=True
    )()

    load_task_product = task(
        task_id="load_products_to_mysql",
        python_callable=load_products_to_mysql,
        provide_context=True
    )()

    wait_for_db >> extract_task_product >> create_table_task_product >> transform_task_product >> load_task_product

etl_postgres_to_mysql_pipeline()