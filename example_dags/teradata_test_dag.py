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
    'retries': 3,
    'retry_delay': timedelta(minutes=15)
    }

dag = DAG('teradata-hook-test', default_args=default_args)

t0 = TeradataOperator(sql="DELETE FROM financial.prueba_final2",
                     task_id='SQL_Delete_Data',
                     teradata_conn_id='td',
                     dag=dag)
t1 = TransferToTeradataOperator(sql="SELECT * FROM mysql.prueba_final2",
                     task_id='Load_to_Teradata',
                     destination_table='financial.prueba_final2',
                     source_conn_id='infobright',
                     destination_conn_id='td',
                     unicode_source=False,
                     dag=dag)
t2 = TeradataOperator(sql="DELETE FROM financial.prueba_final2",
                     task_id='SQL_Delete_Data_2',
                     teradata_conn_id='td',
                     dag=dag)

t3 = TransferToTeradataOperator(sql="SELECT * FROM mysql.prueba_final2",
                     task_id='Load_to_Teradata_Bulk',
                     destination_table='financial.prueba_final2',
                     source_conn_id='infobright',
                     destination_conn_id='td',
                     batch=True,
                     batch_size=3,
                     unicode_source=False,
                     dag=dag)

t4 = TeradataOperator(sql="teradata.sql",
                     task_id='SQL_Delete_Data_3',
                     teradata_conn_id='td',
                     dag=dag)

t5 = TransferToTeradataOperator(sql="SELECT * FROM mysql.prueba_final2",
                     task_id='Load_to_Teradata_Bulk_Unicode',
                     destination_table='financial.prueba_final2',
                     source_conn_id='infobright_jdbc',
                     destination_conn_id='td',
                     batch=True,
                     batch_size=3,
                     dag=dag)

t6 = TeradataOperator(sql="teradata.sql",
                     task_id='SQL_file_test',
                     teradata_conn_id='td',
                     dag=dag)

def get_pandas_df_from_teradata(conn_id, sql):
    conn = TeradataHook(teradata_conn_id=conn_id)
    return conn.get_pandas_df(sql=sql)

t7 = PythonOperator(
    task_id='Dataframe_From_Teradata',
    python_callable=get_pandas_df_from_teradata,
    op_kwargs={
               'conn_id': 'td',
                'sql': 'SELECT * FROM financial.prueba_final2'
               },
    dag=dag)

t0 >> t1 >> t2 >> t3 >> t4 >> t5 >> t6 >> t7