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

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.base_hook import BaseHook
from airflow.contrib.hooks.teradata_hook import TeradataHook


class TransferToTeradataOperator(BaseOperator):
    """
    Moves data from a connection to another, assuming that they both
    provide the required methods in their respective hooks. The source hook
    needs to expose a `get_records` method, and the destination a
    `insert_rows` method.

    This is mean to be used on small-ish datasets that fit in memory.

    :param sql: SQL query to execute against the source database
    :type sql: str
    :param destination_table: target table
    :type destination_table: str
    :param source_conn_id: source connection
    :type source_conn_id: str
    :param destination_conn_id: source connection
    :type destination_conn_id: str
    :param preoperator: sql statement or list of statements to be
        executed prior to loading the data
    :type preoperator: str or list of str
    """

    template_fields = ('sql', 'destination_table', 'preoperator')
    template_ext = ('.sql', '.hql',)
    ui_color = '#ec852a'

    @apply_defaults
    def __init__(
            self,
            sql,
            destination_table,
            source_conn_id,
            destination_conn_id,
            preoperator=None,
            batch=False,
            unicode_source=None,
            batch_size=5000,
            *args, **kwargs):
        super(TransferToTeradataOperator, self).__init__(*args, **kwargs)
        self.sql = sql
        self.destination_table = destination_table
        self.source_conn_id = source_conn_id
        self.destination_conn_id = destination_conn_id
        self.preoperator = preoperator
        self.batch = batch
        self.batch_size = batch_size
        self.unicode_source = unicode_source

    def execute(self, context):
        source_hook = BaseHook.get_hook(self.source_conn_id)

        logging.info("Extracting data from {}".format(self.source_conn_id))
        logging.info("Executing: \n" + self.sql)
        results = source_hook.get_records(self.sql)

        destination_hook = TeradataHook(teradata_conn_id=self.destination_conn_id)
        if self.preoperator:
            logging.info("Running preoperator")
            logging.info(self.preoperator)
            destination_hook.run(self.preoperator)

        if self.batch:
            logging.info("Inserting {} rows into {} with a batch size of {} rows".format(len(results), self.destination_conn_id, self.batch_size))
            destination_hook.bulk_insert_rows(table=self.destination_table, rows=iter(results), commit_every=self.batch_size,  unicode_source=self.unicode_source)
        else:
            logging.info("Inserting {} rows into {}".format(len(results), self.destination_conn_id))
            destination_hook.insert_rows(table=self.destination_table, rows=iter(results), commit_every=1000, unicode_source=self.unicode_source )
