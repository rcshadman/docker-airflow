# -*- coding: utf-8 -*-
from airflow import DAG
from airflow.contrib.operators.transfertoteradata_operator import TransferToTeradataOperator
from airflow.contrib.operators.teradata_operator import TeradataOperator
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.hooks.teradata_hook import TeradataHook
from datetime import datetime, timedelta
d = 1 #Hace cuando dias la tarea se debio ejecutar. Min 1
start = datetime.combine(datetime.today() - timedelta(d), datetime.min.time())
default_args = {
    'owner': 'tests',
    'start_date': start,
    'email': ['felipe.lolas@bci.cl'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
    }

dag = DAG('teradata-test', default_args=default_args, schedule_interval='*/30 * * * *')

t0 = TeradataOperator(sql="teradata.sql",
                     task_id='SQL_file_test',
                     teradata_conn_id='td',
                     wait_for_downstream=True,
                     dag=dag)

t1 = TransferToTeradataOperator(sql="SELECT * FROM mysql.testData",
                     task_id='Load_to_Teradata',
                     destination_table='syslib.testData',
                     source_conn_id='infobright',
                     destination_conn_id='td',
                     unicode_source=False,
                     wait_for_downstream=True,
                     dag=dag)
t2 = TeradataOperator(sql="DELETE FROM syslib.testData",
                     task_id='SQL_Delete_Data_2',
                     wait_for_downstream=True,
                     teradata_conn_id='td',
                     dag=dag)

t3 = TransferToTeradataOperator(sql="SELECT * FROM mysql.testData",
                     task_id='Load_to_Teradata_Bulk',
                     destination_table='syslib.testData',
                     source_conn_id='infobright',
                     destination_conn_id='td',
                     batch=True,
                     batch_size=50,
                     wait_for_downstream=True,
                     unicode_source=False,
                     dag=dag)

t4 = TeradataOperator(sql="DELETE FROM syslib.testData",
                     task_id='SQL_Delete_Data_3',
                     teradata_conn_id='td',
                     wait_for_downstream=True,
                     dag=dag)

t5 = TransferToTeradataOperator(sql="SELECT * FROM mysql.testData",
                     task_id='Load_to_Teradata_Bulk_Unicode',
                     destination_table='syslib.testData',
                     source_conn_id='infobright_jdbc',
                     destination_conn_id='td',
                     batch=True,
                     batch_size=50,
                     wait_for_downstream=True,
                     unicode_source=True,
                     dag=dag)

def get_pandas_df_from_teradata(conn_id, sql):
    conn = TeradataHook(teradata_conn_id=conn_id)
    return conn.get_pandas_df(sql=sql)

t6 = PythonOperator(
    task_id='Dataframe_From_Teradata',
    python_callable=get_pandas_df_from_teradata,
    wait_for_downstream=True,
    op_kwargs={
               'conn_id': 'td',
                'sql': 'SELECT * FROM syslib.testData'
               },
    dag=dag)


t7 = TransferToTeradataOperator(sql="SELECT * FROM mysql.datos",
                     task_id='Load_to_Teradata_Bulk_Split',
                     destination_table='syslib.datos',
                     source_conn_id='infobright',
                     destination_conn_id='td',
                     batch=True,
                     batch_size=50000,
                     wait_for_downstream=True,
                     unicode_source=False,
                     dag=dag)


t0 >> t1 >> t2 >> t3 >> t4 >> t5 >> t6 >> t7