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
    Executes sql code in a specific Oracle database
    :param oracle_conn_id: reference to a specific Oracle database
    :type oracle_conn_id: string
    :param sql: the sql code to be executed
    :type sql: Can receive a str representing a sql statement,
        a list of str (sql statements), or reference to a template file.
        Template reference are recognized by str ending in '.sql'
    """

    template_fields = ('sql',)
    template_ext = ('.sql',)
    ui_color = '#ededed'

    @apply_defaults
    def __init__(
            self, sql, oracle_conn_id='teradata_default', parameters=None,
            autocommit=False, *args, **kwargs):
        super(TeradataOperator, self).__init__(*args, **kwargs)
        self.oracle_conn_id = oracle_conn_id
        self.sql = sql
        self.autocommit = autocommit
        self.parameters = parameters

    def execute(self, context):
        logging.info('Executing: ' + str(self.sql))
        hook = TeradataHook(oracle_conn_id=self.oracle_conn_id)
        hook.run(
            self.sql,
            autocommit=self.autocommit,
            parameters=self.parameters)
