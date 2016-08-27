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

html_template = '''
<table>
        <tr>
               <td>Header</td>
        </tr>
        <tr>
               <td>Content</td>
        </tr>
        <tr>
               <td>Footer</td>
        </tr>
</table>
'''

mail_template = '''Hola como estas <br>'''
t1 = GmailAPISendMailOperator(
    task_id='Send_Mail',
    to=['felipe.lolas@bci.cl','felipe.elias013@gmail.com'],
    sender='felipe.lolas@bci.cl',
    subject='(Airflow) Proceso automatico',
    message=mail_template,
    dag=dag)

t2 = GmailAPISendMailOperator(
    task_id='Send_Mail_Attachment',
    to='felipe.lolas@bci.cl',
    sender='felipe.lolas@bci.cl',
    subject='(Airflow) Proceso automatico: Journey Consumo Attachment',
    html_content=html_template,
    attachment='client_secret.json',
    dag=dag)

t3 = GmailAPISendMailOperator(
    task_id='Send_Mail_Attachment_Multiple',
    to='felipe.lolas@bci.cl',
    sender='felipe.lolas@bci.cl',
    subject='(Airflow) Proceso automatico: Journey Consumo Attachment',
    message=html_template,
    attachment=['client_secret.json','gmail_dag.py'],
    dag=dag)

t1 >> t2
t2 >> t3
