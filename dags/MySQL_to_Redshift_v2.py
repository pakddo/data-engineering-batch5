from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.amazon.aws.transfers.mysql_to_s3 import MySQLToS3Operator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.models import Variable

from datetime import datetime
from datetime import timedelta
from textwrap import dedent
from dateutil.parser import parse

import requests
import logging
import psycopg2
import json



dag = DAG(
    dag_id = 'MySQL_to_Redshift_v2',
    start_date = datetime(2021,9,9), # 날짜가 미래인 경우 실행이 안됨
    schedule_interval = '0 9 * * *',  # 적당히 조절
    max_active_runs = 1,
    catchup = True,
    default_args = {
        'retries': 1,
        'retry_delay': timedelta(minutes=3),
    }
)

schema = "pakddo"
table = "nps"
s3_bucket = "grepp-data-engineering"
date = "{{ ds }}"
s3_key = schema + "-" + table 

# TODO 추가 해야할 로직
# 1. Idempotency 로직이 추가 되어야 함
# 2. key 가 이미 존재 할 경우 처리 불가

mysql_to_s3_nps = MySQLToS3Operator(
    task_id = 'mysql_to_s3_nps',
    query = dedent(f"""SELECT * FROM prod.nps 
    WHERE created_at >= DATE('{date}')
      AND created_at < DATE_ADD(DATE('{date}'), INTERVAL 1 DAY)
    """),
    s3_bucket = s3_bucket,
    s3_key = s3_key + '/' + date,
    mysql_conn_id = "mysql_conn_id",
    aws_conn_id = "aws_conn_id",
    verify = False,
    dag = dag
)

s3_to_redshift_nps = S3ToRedshiftOperator(
    task_id = 's3_to_redshift_nps',
    s3_bucket = s3_bucket,
    s3_key = s3_key + '/' + date,
    schema = schema,
    table = table,
    copy_options=['csv'],
    redshift_conn_id = "redshift_dev_db",
    dag = dag
)

mysql_to_s3_nps >> s3_to_redshift_nps
