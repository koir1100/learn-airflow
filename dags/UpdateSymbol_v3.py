from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta

import psycopg2

import yfinance as yf

import logging

def get_Redshift_connection(autocommit=False):
    hook = PostgresHook(postgres_conn_id='redshift_dev_db')
    conn = hook.get_conn()
    conn.autocommit = autocommit
    return conn.cursor()


@task
def get_historical_prices(symbol):
    ticket = yf.Ticker(symbol)
    data = ticket.history()
    records = []

    for index, row in data.iterrows():
        date = index.strftime('%Y-%m-%d %H:%M:%S')
        records.append([date, row["Open"], row["High"], row["Low"], row["Close"], row["Volume"]])
    
    return records
    

@task
def load(schema, table, records):
    cur = get_Redshift_connection()

    # 원본 테이블이 없다면 생성
    create_table_sql = f"""
CREATE TABLE IF NOT EXISTS {schema}.{table} (
    date date,
    "open" float,
    high float,
    low float,
    close float,
    volume bigint,
    created_date timestamp default GETDATE()
);"""
    logging.info(create_table_sql)

    # 임시 테이블 생성
    create_temp_sql = f"""CREATE TEMP TABLE t AS SELECT * FROM {schema}.{table};"""
    logging.info(create_temp_sql)
    try:
        cur.execute(create_table_sql)
        cur.execute(create_temp_sql)
        cur.execute("COMMIT;")
    except (Exception, psycopg2.DatabaseError) as error:
        cur.execute("ROLLBACK;")
        logging.error(error)
        raise

    # 임시 테이블 데이터 입력
    insert_sql = f"INSERT INTO t VALUES " + ",".join(records)
    logging.info(insert_sql)
    try:
        cur.execute(insert_sql)
        cur.execute("COMMIT;")
    except (Exception, psycopg2.DatabaseError) as error:
        cur.execute("ROLLBACK;")
        logging.error(error)
        raise

    # 기존 테이블 대체
    alter_sql = f"""DELETE FROM {schema}.{table};
INSERT INTO {schema}.{table}
    SELECT
        date, "open", high, low, close, volume
    FROM
    (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY date ORDER BY created_date DESC) AS seq
        FROM t
    )
    WHERE seq = 1;"""
    logging.info(alter_sql)

    try:
        cur.execute(alter_sql)
        cur.execute("COMMIT;")
    except (Exception, psycopg2.DatabaseError) as error:
        cur.execute("ROLLBACK;")
        logging.error(error)
        raise
    logging.info("load done")

with DAG(
    dag_id='UpdateSymbol_v3',
    start_date=datetime(2024,5,22),
    catchup=False,
    schedule='0 10 * * *',
    tags=['API'],
    max_active_runs=1,
    default_args={
        'retries': 1,
        'retry_delay': timedelta(minutes=1),
    }
) as dag:
    
    results = get_historical_prices("AAPL")
    load("yonggu_choi_14", "stock_info_v3", results)
