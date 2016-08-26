from airflow import DAG
from airflow.contrib.operators.gmail_operator import GmailAPISendMailOperator
from datetime import datetime, timedelta
seven_days_ago = datetime.combine(datetime.today() - timedelta(1),
                                  datetime.min.time())

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': seven_days_ago,
    'email': ['airflow@airflow.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('example_gmail_api', default_args=default_args)

t1 = GmailAPISendMailOperator(
    task_id='Send_Mail',
    to='felipe.lolas@bci.cl',
    sender='felipe.lolas@bci.cl',
    message='prueba',
    subject='prueba',
    credentials_file='credentials.json',
    dag=dag)