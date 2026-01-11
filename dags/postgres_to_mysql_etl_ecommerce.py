from airflow.decorators import dag, task
from airflow.sensors.sql import SqlSensor
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.mysql.hooks.mysql import MySqlHook
from datetime import datetime
import pandas as pd


# ===================== PRODUCTS =====================
def extract_productsperkategori_from_postgres(**context):
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
    data = df.to_dict(orient="records")

    ti = context["ti"]
    ti.xcom_push(key="raw_products", value=data)
    print(f"Extracted {len(df)} rows from PostgreSQL")


def create_mysql_table_productperkategori(**context):
    mysql_hook = MySqlHook(mysql_conn_id="local_mysql")

    mysql_hook.run("""
        CREATE TABLE IF NOT EXISTS jumlah_produk_per_kategori (
            category VARCHAR(255) PRIMARY KEY,
            total_products INT NOT NULL
        );
    """)


def transform_productsperkategori(**context):
    ti = context["ti"]
    raw_data = ti.xcom_pull(task_ids="extract_products", key="raw_products")

    if not raw_data:
        raise ValueError("XCom raw_products kosong")

    df = pd.DataFrame(raw_data)
    df["category"] = df["category"].str.lower()
    df["total_products"] = df["total_products"].astype(int)

    ti.xcom_push(
        key="transformed_products",
        value=df.to_dict(orient="records")
    )


def load_products_to_mysql(**context):
    ti = context["ti"]
    data = ti.xcom_pull(task_ids="transform_products", key="transformed_products")

    if not data:
        raise ValueError("XCom transformed_products kosong")

    df = pd.DataFrame(data)
    mysql_hook = MySqlHook(mysql_conn_id="local_mysql")
    engine = mysql_hook.get_sqlalchemy_engine()

    df.to_sql(
        name="jumlah_produk_per_kategori",
        con=engine,
        if_exists="append",
        index=False,
        chunksize=1000,
        method="multi"
    )


# ===================== CUSTOMERS =====================
def extract_countcustomercountry_from_postgres(**context):
    postgres_hook = PostgresHook(postgres_conn_id="dibimbingconn")

    sql = """
        SELECT
            country,
            COUNT(*) AS total_customers
        FROM raw_customers
        GROUP BY country
        ORDER BY total_customers DESC;
    """

    df = postgres_hook.get_pandas_df(sql)

    ti = context["ti"]
    ti.xcom_push(key="raw_customers", value=df.to_dict(orient="records"))
    print(f"Extracted {len(df)} rows from PostgreSQL")


def create_mysql_table_countcustomercountry(**context):
    mysql_hook = MySqlHook(mysql_conn_id="local_mysql")

    mysql_hook.run("""
        CREATE TABLE IF NOT EXISTS jumlah_pelanggan_per_negara (
            country VARCHAR(255),
            total_customers INT
        );
    """)


def transform_customerspercountry(**context):
    ti = context["ti"]
    raw_data = ti.xcom_pull(task_ids="extract_customers", key="raw_customers")

    if not raw_data:
        raise ValueError("XCom raw_customers kosong")

    df = pd.DataFrame(raw_data)
    df["country"] = df["country"].str.lower()
    df["total_customers"] = df["total_customers"].astype(int)

    ti.xcom_push(
        key="transformed_customers",
        value=df.to_dict(orient="records")
    )


def load_customerspercountry_to_mysql(**context):
    ti = context["ti"]
    data = ti.xcom_pull(task_ids="transform_customers", key="transformed_customers")

    if not data:
        raise ValueError("XCom transformed_customers kosong")

    df = pd.DataFrame(data)
    mysql_hook = MySqlHook(mysql_conn_id="local_mysql")
    engine = mysql_hook.get_sqlalchemy_engine()

    df.to_sql(
        name="jumlah_pelanggan_per_negara",
        con=engine,
        if_exists="append",
        index=False,
        chunksize=1000,
        method="multi"
    )


# ===================== DAG =====================
@dag(
    dag_id="dag_ETL_warehouse_postgres_to_mysql",
    schedule_interval=None,
    start_date=datetime(2024, 8, 1),
    catchup=False,
    tags=["data-engineering", "etl"]
)
def etl_postgres_to_mysql_pipeline():

    wait_for_db = SqlSensor(
        task_id="wait_for_db",
        conn_id="dibimbingconn",
        sql="SELECT 1 FROM raw_products LIMIT 1;",
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



    extract_task_customer = task(
    task_id="extract_customers",
    python_callable=extract_countcustomercountry_from_postgres,
    provide_context=True
    )()

    create_table_task_customer = task(
        task_id="create_mysql_table_customers",
        python_callable=create_mysql_table_countcustomercountry,
        provide_context=True
    )()

    transform_task_customer = task(
        task_id="transform_customers",
        python_callable=transform_customerspercountry,
        provide_context=True
    )()

    load_task_customer = task(
        task_id="load_customers_to_mysql",
        python_callable=load_customerspercountry_to_mysql,
        provide_context=True
    )()
    wait_for_db >> extract_task_product >> create_table_task_product \
    >> transform_task_product >> load_task_product \
    >> extract_task_customer >> create_table_task_customer \
    >> transform_task_customer >> load_task_customer

etl_postgres_to_mysql_pipeline()
