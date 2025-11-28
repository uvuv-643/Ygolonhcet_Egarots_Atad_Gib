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


def extract_trains(**context):
    url = Variable.get("TRAIN_URL", default_var="https://travel.yandex.ru/trains/moscow--saint-petersburg/?when=2025-11-30")
    server_url = Variable.get("TRAIN_SERVER_URL", default_var="http://host.docker.internal:8888")
    print("Request to server: ", f"{server_url}/?url={encoded_url}&num_trains=3")
    encoded_url = quote(url, safe='')
    response = requests.get(f"{server_url}/?url={encoded_url}&num_trains=3", timeout=300)
    response.raise_for_status()
    trains = response.json()
    
    if isinstance(trains, dict) and 'error' in trains:
        raise Exception(f"Server error: {trains['error']}")
    
    logging.info("Extracted %d trains", len(trains))
    return trains


def transform_trains(**context):
    ti = context["ti"]
    raw_trains = ti.xcom_pull(task_ids="extract_trains") or []
    
    transformed = []
    for train in raw_trains:
        duration = train.get('duration', {})
        transformed.append({
            'train_number': train.get('train_number'),
            'train_name': train.get('train_name'),
            'carrier': train.get('carrier'),
            'departure_station': train.get('departure_station'),
            'arrival_station': train.get('arrival_station'),
            'departure_date': train.get('departure_date'),
            'departure_time': train.get('departure_time'),
            'arrival_date': train.get('arrival_date'),
            'arrival_time': train.get('arrival_time'),
            'duration_hours': duration.get('hours', 0),
            'duration_minutes': duration.get('minutes', 0),
            'min_price': train.get('min_price', 0.0),
            'raw': train
        })
    
    logging.info("Transformed %d trains", len(transformed))
    return transformed


def load_trains_to_postgres(**context):
    ti = context["ti"]
    rows = ti.xcom_pull(task_ids="transform_trains") or []
    if not rows:
        logging.info("No rows to load into Postgres")
        return
    
    hook = PostgresHook(postgres_conn_id="postgres_ods")
    conn = hook.get_conn()
    cur = conn.cursor()
    
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS ods_trains_basic_pg (
            train_number VARCHAR(50) PRIMARY KEY,
            train_name TEXT,
            carrier TEXT,
            departure_station TEXT,
            arrival_station TEXT,
            departure_date TEXT,
            departure_time TEXT,
            arrival_date TEXT,
            arrival_time TEXT,
            duration_hours INT,
            duration_minutes INT,
            min_price DECIMAL(10,2),
            raw JSONB
        );
        """
    )
    
    insert_sql = """
        INSERT INTO ods_trains_basic_pg 
        (train_number, train_name, carrier, departure_station, arrival_station, 
         departure_date, departure_time, arrival_date, arrival_time, 
         duration_hours, duration_minutes, min_price, raw)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
        ON CONFLICT (train_number) DO UPDATE
        SET train_name = EXCLUDED.train_name,
            carrier = EXCLUDED.carrier,
            departure_station = EXCLUDED.departure_station,
            arrival_station = EXCLUDED.arrival_station,
            departure_date = EXCLUDED.departure_date,
            departure_time = EXCLUDED.departure_time,
            arrival_date = EXCLUDED.arrival_date,
            arrival_time = EXCLUDED.arrival_time,
            duration_hours = EXCLUDED.duration_hours,
            duration_minutes = EXCLUDED.duration_minutes,
            min_price = EXCLUDED.min_price,
            raw = EXCLUDED.raw;
    """
    
    for r in rows:
        cur.execute(
            insert_sql,
            (
                r["train_number"],
                r["train_name"],
                r["carrier"],
                r["departure_station"],
                r["arrival_station"],
                r["departure_date"],
                r["departure_time"],
                r["arrival_date"],
                r["arrival_time"],
                r["duration_hours"],
                r["duration_minutes"],
                r["min_price"],
                json.dumps(r["raw"], ensure_ascii=False),
            ),
        )
    
    conn.commit()
    cur.close()
    conn.close()
    logging.info("Loaded %d trains into Postgres", len(rows))


def load_trains_to_mysql(**context):
    ti = context["ti"]
    rows = ti.xcom_pull(task_ids="transform_trains") or []
    if not rows:
        logging.info("No rows to load into MySQL")
        return
    
    hook = MySqlHook(mysql_conn_id="mysql_ods")
    conn = hook.get_conn()
    cur = conn.cursor()
    
    # Устанавливаем кодировку для соединения
    cur.execute("SET NAMES utf8mb4 COLLATE utf8mb4_unicode_ci")
    cur.execute("SET CHARACTER SET utf8mb4")
    
    # Проверяем существование таблицы и изменяем кодировку, если нужно
    cur.execute("""
        SELECT COUNT(*) FROM information_schema.tables 
        WHERE table_schema = DATABASE() AND table_name = 'ods_trains_basic_mysql'
    """)
    table_exists = cur.fetchone()[0] > 0
    
    if table_exists:
        # Изменяем кодировку существующей таблицы
        cur.execute("ALTER TABLE ods_trains_basic_mysql CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")
    
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS ods_trains_basic_mysql (
            train_number VARCHAR(50) PRIMARY KEY,
            train_name VARCHAR(500),
            carrier VARCHAR(255),
            departure_station VARCHAR(255),
            arrival_station VARCHAR(255),
            departure_date VARCHAR(50),
            departure_time VARCHAR(50),
            arrival_date VARCHAR(50),
            arrival_time VARCHAR(50),
            duration_hours INT,
            duration_minutes INT,
            min_price DECIMAL(10,2),
            raw JSON
        ) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
        """
    )
    
    insert_sql = """
        INSERT INTO ods_trains_basic_mysql
        (train_number, train_name, carrier, departure_station, arrival_station,
         departure_date, departure_time, arrival_date, arrival_time,
         duration_hours, duration_minutes, min_price, raw)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CAST(%s AS JSON))
        ON DUPLICATE KEY UPDATE
            train_name = VALUES(train_name),
            carrier = VALUES(carrier),
            departure_station = VALUES(departure_station),
            arrival_station = VALUES(arrival_station),
            departure_date = VALUES(departure_date),
            departure_time = VALUES(departure_time),
            arrival_date = VALUES(arrival_date),
            arrival_time = VALUES(arrival_time),
            duration_hours = VALUES(duration_hours),
            duration_minutes = VALUES(duration_minutes),
            min_price = VALUES(min_price),
            raw = VALUES(raw);
    """
    
    for r in rows:
        cur.execute(
            insert_sql,
            (
                r["train_number"],
                r["train_name"],
                r["carrier"],
                r["departure_station"],
                r["arrival_station"],
                r["departure_date"],
                r["departure_time"],
                r["arrival_date"],
                r["arrival_time"],
                r["duration_hours"],
                r["duration_minutes"],
                r["min_price"],
                json.dumps(r["raw"], ensure_ascii=False),
            ),
        )
    
    conn.commit()
    cur.close()
    conn.close()
    logging.info("Loaded %d trains into MySQL", len(rows))


def load_trains_to_mongo(**context):
    ti = context["ti"]
    rows = ti.xcom_pull(task_ids="transform_trains") or []
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
    collection = db["ods_trains_basic"]
    
    for r in rows:
        doc = r["raw"]
        doc["_id"] = r["train_number"]
        collection.replace_one({"_id": doc["_id"]}, doc, upsert=True)
    
    logging.info("Loaded %d trains into MongoDB", len(rows))


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="trains_basic",
    default_args=default_args,
    description="Загрузка базовой информации о поездах во все БД (PostgreSQL, MySQL, MongoDB)",
    start_date=datetime(2025, 1, 1),
    schedule_interval="5,20,35,50 * * * *",  # Каждые 15 минут, начиная с 05:00
    catchup=False,
    max_active_runs=1,
    tags=["trains", "basic", "ods", "all-databases"],
) as dag:
    extract = PythonOperator(
        task_id="extract_trains",
        python_callable=extract_trains,
    )
    
    transform = PythonOperator(
        task_id="transform_trains",
        python_callable=transform_trains,
    )
    
    load_postgres = PythonOperator(
        task_id="load_to_postgres",
        python_callable=load_trains_to_postgres,
    )
    
    load_mysql = PythonOperator(
        task_id="load_to_mysql",
        python_callable=load_trains_to_mysql,
    )
    
    load_mongo = PythonOperator(
        task_id="load_to_mongo",
        python_callable=load_trains_to_mongo,
    )
    
    # Extract -> Transform -> Load (параллельно во все БД)
    extract >> transform >> [load_postgres, load_mysql, load_mongo]
