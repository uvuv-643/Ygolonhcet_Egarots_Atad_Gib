from datetime import datetime, timedelta
import json
import logging

from airflow.hooks.base import BaseHook
from pymongo import MongoClient
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.models import Variable
import requests
from urllib.parse import quote


def extract_cars(**context):
    url = Variable.get("TRAIN_URL", default_var="https://travel.yandex.ru/trains/moscow--saint-petersburg/?when=2025-11-28")
    server_url = Variable.get("TRAIN_SERVER_URL", default_var="http://host.docker.internal:8888")
    
    encoded_url = quote(url, safe='')
    response = requests.get(f"{server_url}/?url={encoded_url}&num_trains=3", timeout=300)
    response.raise_for_status()
    trains = response.json()
    
    if isinstance(trains, dict) and 'error' in trains:
        raise Exception(f"Server error: {trains['error']}")
    
    all_cars = []
    for train in trains:
        train_number = train.get('train_number', 'unknown')
        cars = train.get('cars', [])
        for car in cars:
            car['train_number'] = train_number
            all_cars.append(car)
    
    logging.info("Extracted %d cars", len(all_cars))
    return all_cars


def transform_cars(**context):
    ti = context["ti"]
    raw_cars = ti.xcom_pull(task_ids="extract_cars") or []
    
    transformed = []
    for car in raw_cars:
        transformed.append({
            'train_number': car.get('train_number'),
            'car_number': car.get('car_number'),
            'carrier': car.get('carrier'),
            'car_class': car.get('car_class'),
            'details': car.get('details'),
            'raw': car
        })
    
    logging.info("Transformed %d cars", len(transformed))
    return transformed


def load_cars_to_postgres(**context):
    ti = context["ti"]
    rows = ti.xcom_pull(task_ids="transform_cars") or []
    if not rows:
        logging.info("No rows to load into Postgres")
        return
    
    hook = PostgresHook(postgres_conn_id="postgres_ods")
    conn = hook.get_conn()
    cur = conn.cursor()
    
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS ods_trains_cars_pg (
            id SERIAL PRIMARY KEY,
            train_number VARCHAR(50),
            car_number INT,
            carrier TEXT,
            car_class TEXT,
            details TEXT,
            raw JSONB
        );
        """
    )
    
    insert_sql = """
        INSERT INTO ods_trains_cars_pg 
        (train_number, car_number, carrier, car_class, details, raw)
        VALUES (%s, %s, %s, %s, %s, %s::jsonb);
    """
    
    for r in rows:
        cur.execute(
            insert_sql,
            (
                r["train_number"],
                r["car_number"],
                r["carrier"],
                r["car_class"],
                r["details"],
                json.dumps(r["raw"], ensure_ascii=False),
            ),
        )
    
    conn.commit()
    cur.close()
    conn.close()
    logging.info("Loaded %d cars into Postgres", len(rows))


def load_cars_to_mysql(**context):
    ti = context["ti"]
    rows = ti.xcom_pull(task_ids="transform_cars") or []
    if not rows:
        logging.info("No rows to load into MySQL")
        return
    
    hook = MySqlHook(mysql_conn_id="mysql_ods")
    conn = hook.get_conn()
    cur = conn.cursor()
    
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS ods_trains_cars_mysql (
            id INT AUTO_INCREMENT PRIMARY KEY,
            train_number VARCHAR(50),
            car_number INT,
            carrier VARCHAR(255),
            car_class VARCHAR(255),
            details TEXT,
            raw JSON
        );
        """
    )
    
    insert_sql = """
        INSERT INTO ods_trains_cars_mysql
        (train_number, car_number, carrier, car_class, details, raw)
        VALUES (%s, %s, %s, %s, %s, CAST(%s AS JSON));
    """
    
    for r in rows:
        cur.execute(
            insert_sql,
            (
                r["train_number"],
                r["car_number"],
                r["carrier"],
                r["car_class"],
                r["details"],
                json.dumps(r["raw"], ensure_ascii=False),
            ),
        )
    
    conn.commit()
    cur.close()
    conn.close()
    logging.info("Loaded %d cars into MySQL", len(rows))


def load_cars_to_mongo(**context):
    ti = context["ti"]
    rows = ti.xcom_pull(task_ids="transform_cars") or []
    if not rows:
        logging.info("No rows to load into MongoDB")
        return
    
    conn = BaseHook.get_connection("mongo_ods")
    extras = conn.extra_dejson or {}
    
    host = conn.host or "mongo_ods"
    port = conn.port or 27017
    
    auth = ""
    if conn.login:
        auth = conn.login
        if conn.password:
            auth += f":{conn.password}"
        auth += "@"
    
    uri = f"mongodb://{auth}{host}:{port}"
    
    client = MongoClient(uri)
    db_name = extras.get("database", "ods_trains")
    db = client[db_name]
    collection = db["ods_trains_cars"]
    
    for r in rows:
        doc = r["raw"]
        doc["_id"] = f"{r['train_number']}_{r['car_number']}"
        collection.replace_one({"_id": doc["_id"]}, doc, upsert=True)
    
    logging.info("Loaded %d cars into MongoDB", len(rows))


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="trains_cars_to_postgres",
    default_args=default_args,
    description="Загрузка информации о вагонах в ODS (PostgreSQL)",
    start_date=datetime(2025, 1, 1),
    schedule_interval="0 */6 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["trains", "cars", "ods", "postgres"],
) as dag_postgres:
    extract_pg = PythonOperator(
        task_id="extract_cars",
        python_callable=extract_cars,
    )
    
    transform_pg = PythonOperator(
        task_id="transform_cars",
        python_callable=transform_cars,
    )
    
    load_pg = PythonOperator(
        task_id="load_cars_to_postgres",
        python_callable=load_cars_to_postgres,
    )
    
    extract_pg >> transform_pg >> load_pg


with DAG(
    dag_id="trains_cars_to_mysql",
    default_args=default_args,
    description="Загрузка информации о вагонах в ODS (MySQL)",
    start_date=datetime(2025, 1, 1),
    schedule_interval="15 */6 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["trains", "cars", "ods", "mysql"],
) as dag_mysql:
    extract_mysql = PythonOperator(
        task_id="extract_cars",
        python_callable=extract_cars,
    )
    
    transform_mysql = PythonOperator(
        task_id="transform_cars",
        python_callable=transform_cars,
    )
    
    load_mysql = PythonOperator(
        task_id="load_cars_to_mysql",
        python_callable=load_cars_to_mysql,
    )
    
    extract_mysql >> transform_mysql >> load_mysql


with DAG(
    dag_id="trains_cars_to_mongo",
    default_args=default_args,
    description="Загрузка информации о вагонах в ODS (MongoDB)",
    start_date=datetime(2025, 1, 1),
    schedule_interval="30 */6 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["trains", "cars", "ods", "mongo"],
) as dag_mongo:
    extract_mongo = PythonOperator(
        task_id="extract_cars",
        python_callable=extract_cars,
    )
    
    transform_mongo = PythonOperator(
        task_id="transform_cars",
        python_callable=transform_cars,
    )
    
    load_mongo = PythonOperator(
        task_id="load_cars_to_mongo",
        python_callable=load_cars_to_mongo,
    )
    
    extract_mongo >> transform_mongo >> load_mongo

