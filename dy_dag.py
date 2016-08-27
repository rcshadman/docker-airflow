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
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta

seven_days_ago = datetime.combine(datetime.today() - timedelta(1),
                                  datetime.min.time())
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': seven_days_ago,
}

dag = DAG('dyn-task', default_args=default_args)
Start = DummyOperator(task_id="Start",
                         dag=dag)

End = DummyOperator(task_id="End",
                         dag=dag)

for k in range(1,10):
    task = DummyOperator(task_id="Loop_Task_" +str(k),
                         dag=dag)
    Start >> task
    task >> End