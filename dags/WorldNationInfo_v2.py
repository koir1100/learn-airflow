from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.snowflake.transfers.copy_into_snowflake import CopyFromExternalStageToSnowflakeOperator
from airflow.providers.amazon.aws.transfers.sql_to_s3 import SqlToS3Operator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.providers.amazon.aws.operators.s3 import S3DeleteObjectsOperator

from datetime import datetime
from datetime import timedelta

dag = DAG(
    dag_id = 'WorldNationInfo_v2',
    start_date = datetime(2024,5,30), # 날짜가 미래인 경우 실행이 안됨
    schedule = '0 9 * * *',  # 적당히 조절
    max_active_runs = 1,
    catchup = False,
    default_args = {
        'retries': 1,
        'retry_delay': timedelta(minutes=3),
    }
)

schema = "yonggu_choi_14"
table = "nation_info"
s3_bucket = "yonggu-practice-bucket"
s3_key = "nation-info"
s3_new_key = "nation-info/nation_info.csv"

execute_query_first = SQLExecuteQueryOperator(
    task_id = "execute_reset_query",
    conn_id = "snowflake_conn_id",
    sql = "DELETE FROM dev.public.raw_source;",
    autocommit = True,
    split_statements = True,
    return_last = False,
    dag = dag
)

copy_into_table = CopyFromExternalStageToSnowflakeOperator(
    task_id = "s3_copy_into_table",
    #files = ["nation_list.json"],
    snowflake_conn_id = "snowflake_conn_id",
    warehouse = "COMPUTE_WH",
    database = "dev",
    schema = "public",
    table = "raw_source",
    stage = '\"DEV\".\"PUBLIC\".\"NATION_INFO_STAGE\"',
    role = "accountadmin",
    file_format = "(type = 'json')",
    copy_options = "force = TRUE",
    pattern = ".*[.]json",
    dag = dag
)

s3_delete_origin_file = S3DeleteObjectsOperator(
    task_id = "s3_delete_origin_file",
    bucket = s3_bucket,
    keys = "nation-info/nation_list.json",
    aws_conn_id = "aws_conn_id_choi",
    verify = False,
    dag = dag
)

full_update_query = """CREATE OR REPLACE TABLE dev.public.nation_info AS
(
    SELECT
        t.VALUE:name:official::string AS COUNTRY,
        t.VALUE:area::float AS area,
        t.VALUE:population::int AS population
    FROM
        (SELECT SRC FROM dev.public.raw_source) AS s,
        TABLE(flatten(s.SRC, recursive => true)) t
    WHERE COUNTRY IS NOT NULL
);"""

execute_query_second = SQLExecuteQueryOperator(
    task_id = "execute_full_update_query",
    conn_id = "snowflake_conn_id",
    sql = full_update_query,
    autocommit = True,
    split_statements = True,
    return_last = False,
    dag = dag
)

# query_test = """select current_date() as date;"""
query = """SELECT COUNTRY, AREA, POPULATION FROM dev.public.nation_info;"""

snowflake_to_s3_nation_list = SqlToS3Operator(
    task_id = "snowflake_to_s3_nation_list",
    query = query,
    s3_bucket = s3_bucket,
    s3_key = s3_new_key,
    sql_conn_id = "snowflake_conn_id",
    aws_conn_id = "aws_conn_id_choi",
    verify = False,
    replace = True,
    pd_kwargs = {"index": False, "header": True},    
    dag = dag
)

s3_to_redshift_nation_info = S3ToRedshiftOperator(
    task_id = 's3_to_redshift_nation_info',
    s3_bucket = s3_bucket,
    s3_key = s3_key,
    schema = schema,
    table = table,
    column_list = ["country", "area", "population"],
    copy_options=["csv", "IGNOREHEADER AS 1", "QUOTE AS '\"'", "DELIMITER ','"],
    redshift_conn_id = "redshift_dev_db",
    aws_conn_id = "aws_conn_id_choi",
    method = "UPSERT",
    upsert_keys = ["country"],
    dag = dag
)

execute_query_first >> copy_into_table >> s3_delete_origin_file \
>> execute_query_second >> snowflake_to_s3_nation_list >> s3_to_redshift_nation_info
