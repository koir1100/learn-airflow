from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook

from datetime import date, datetime
from datetime import timedelta

import json
import requests
import logging

def get_Redshift_connection(autocommit=True):
    hook = PostgresHook(postgres_conn_id='redshift_dev_db')
    conn = hook.get_conn()
    conn.autocommit = autocommit
    return conn.cursor()

@task
def extract(url):
    logging.info(datetime.now(datetime.UTC))
    full_info = requests.get(url)
    return full_info.text

@task
def transform(text):
    dict_info = json.loads(text)
    new_data = []
    for idx, info in enumerate(dict_info):
        new_data.append([idx, info["name"]["official"], info["population"], info["area"]])
    
    # new_data.sort(key=lambda x: x[0])
    # print(*new_data, sep='\n')
    logging.info("Transform ended")
    return new_data

@task
def load(schema, table_info, records):
    logging.info("load started")
    cur = get_Redshift_connection()

    try:
        cur.execute("BEGIN;")
        cur.execute(f"""
CREATE TABLE IF NOT EXIST {schema}.{table_info[0]} (
    {table_info[1]}
);""")
        cur.execute(f"DELETE FROM {schema}.{table_info[0]};")
        for record in records:
            sql = f"INSERT INTO {schema}.{table_info[0]} VALUES ('{record[0]}', '{record[table_info[2]]}');"
            print(sql)
            cur.execute(sql)
        cur.execute("COMMIT;")
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        cur.execute("ROLLBACK")
        raise
    logging.info("load done")

    with DAG(
        dag_id='WorldNationInfo',
        start_date=date(date.today - timedelta(days=1)),
        schedule='30 6 * * 6',
        max_active_runs=1,
        catchup=False,
        default_args={
            'retries': 1,
            'retry_delay': timedelta(minutes=3),
        }
    ) as dag:
        
        url = "https://restcountries.com/v3.1/all?fields=name,population,area"
        schema = 'yonggu_choi_14'
        table_info = [
            ["country", "id int, country_name text", 1], 
            ["population", "id int, population int", 2], 
            ["area", "id int, area float", 3],
        ]

        lines = transform(extract(url))
        for info in table_info:
            load(schema, info, lines)