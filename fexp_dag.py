# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""
### Example HTTP operator and sensor
"""
from airflow import DAG
from airflow.operators.docker_operator import DockerOperator
from datetime import datetime, timedelta

seven_days_ago = datetime.combine(datetime.today() - timedelta(1),
                                  datetime.min.time())
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': seven_days_ago,
}

dag = DAG('docker-fexp', default_args=default_args)
select_query = "SELECT * FROM financial.trans"
t1 = DockerOperator(
         docker_url='192.168.1.63:2375',
         image='bci/teradata-tdexp:15.10',
         task_id='Descarga_CSV_Teradata',
         xcom_push=True,
         xcom_all=True,
         environment={
             'HOST': '192.168.1.64',
             'USERNAME': 'dbc',
             'PASSWORD': 'dbc',
             'FILENAME': '/opt/results/jen.csv'
         },
         command=select_query.replace("'","\\'"),
         volumes=['/opt/airflow/files/docker:/opt/results'],
         destroy_on_finish=True,
         dag=dag)