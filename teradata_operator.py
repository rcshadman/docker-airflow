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

import logging

from airflow.contrib.hooks.teradata_hook import TeradataHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class TeradataOperator(BaseOperator):
    """
    Executes sql code in a specific teradata database
    :param teradata_conn_id: reference to a specific teradata database
    :type teradata_conn_id: string
    :param sql: the sql code to be executed
    :type sql: Can receive a str representing a sql statement,
        a list of str (sql statements), or reference to a template file.
        Template reference are recognized by str ending in '.sql'
    """

    template_fields = ('sql',)
    template_ext = ('.sql',)
    ui_color = '#f6c79a'

    @apply_defaults
    def __init__(
            self, sql, teradata_conn_id='teradata_default', *args, **kwargs):
        super(TeradataOperator, self).__init__(*args, **kwargs)
        self.teradata_conn_id = teradata_conn_id
        self.sql = sql

    def execute(self, context):
        logging.info('Executing: ' + str(self.sql))
        hook = TeradataHook(teradata_conn_id=self.teradata_conn_id)
        hook.run(self.sql)