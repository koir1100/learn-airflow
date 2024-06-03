from airflow import DAG
from airflow.decorators import task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook

from datetime import datetime
from datetime import timedelta

import json
import logging
import psycopg2
import requests
from requests.exceptions import HTTPError
from http import HTTPStatus

from botocore.exceptions import ClientError, ValidationError

def get_Redshift_connection(autocommit=True):
    hook = PostgresHook(postgres_conn_id='redshift_dev_db')
    conn = hook.get_conn()
    conn.autocommit = autocommit
    return conn.cursor()

def get_S3_session():
    hook = S3Hook(aws_conn_id='aws_conn_id_choi')
    session = hook.get_session(region_name="ap-northeast-2")
    return session

def get_generated_presigned_url(session):
    key_name = "nation-info/nation_list.json"
    s3_client = session.client('s3')
    url = ""

    try:
        url = s3_client.generate_presigned_url(
            ClientMethod='get_object',
            Params = {
                'Bucket': "yonggu-practice-bucket",
                'Key': key_name,
            },
            # url 유효기간 (단위:second)
            ExpiresIn = 10
        )
        logging.info(f"generated url: {url}")
    except ClientError as e:
        logging.error(e)
        raise ValidationError({"s3": ["S3 Client Error"]})

    return url

@task
def extract():
    logging.info(datetime.now())
    url = get_generated_presigned_url(get_S3_session())

    try:
        full_info = requests.get(url)
        if full_info.status_code != HTTPStatus.OK:
            raise full_info.raise_for_status()
    except HTTPError as e:
        logging.error(e)
        raise

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
CREATE TABLE IF NOT EXISTS {schema}.{table_info[0]} (
    {table_info[1]}
);""")
        cur.execute(f"DELETE FROM {schema}.{table_info[0]};")
        for record in records:
            sql = f"INSERT INTO {schema}.{table_info[0]} VALUES (%s::int, %s);"
            print(sql, (record[0], record[table_info[2]]))
            cur.execute(sql, (record[0], record[table_info[2]]))
        cur.execute("COMMIT;")
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        cur.execute("ROLLBACK;")
        raise
    logging.info("load done")


with DAG(
    dag_id='WorldNationInfo',
    start_date=datetime(2024,5,25),
    schedule='30 6 * * 6',
    max_active_runs=1,
    catchup=False,
    default_args={
        'retries': 1,
        'retry_delay': timedelta(minutes=3),
    }
) as dag:

    #url = "https://restcountries.com/v3.1/all?fields=name,population,area"
    schema = 'yonggu_choi_14'
    table_info = [
        ["country", "id int, country_name text", 1], 
        ["population", "id int, population int", 2], 
        ["area", "id int, area float", 3],
    ]

    lines = transform(extract())
    for info in table_info:
        load(schema, info, lines)

