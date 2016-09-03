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

import teradata

from airflow.hooks.dbapi_hook import DbApiHook
from builtins import str
import sys
from past.builtins import basestring
from datetime import datetime
import numpy
import logging


class TeradataHook(DbApiHook):
    """
    Interact with Teradata SQL.
    """
    conn_name_attr = 'teradata_conn_id'
    default_conn_name = 'teradata_default'
    supports_autocommit = True

    def get_conn(self):
        """
        Returns a teradata connection object
        Optional parameters for using a custom DSN connection (instead of using a server alias from tnsnames.ora)
        The dsn (data source name) is the TNS entry (from the Teradata names server or tnsnames.ora file)
        or is a string like the one returned from makedsn().
        :param dsn: the host address for the Teradata server
        :param service_name: the db_unique_name of the database that you are connecting to (CONNECT_DATA part of TNS)
        You can set these parameters in the extra fields of your connection
        as in ``{ "dsn":"some.host.address" , "service_name":"some.service.name" }``
        """
        conn = self.get_connection(self.teradata_conn_id)
        externalDsn = conn.extra_dejson.get('externalDsn', None)
        appn = conn.extra_dejson.get('appName', 'airflow')
        ver = conn.extra_dejson.get('version', '1.0')
        log = conn.extra_dejson.get('logging', False)

        conn = teradata.UdaExec(appName=appn,
                                version=ver,
                                logConsole=False,
                                configureLogging=False,
                                checkpointFile=False).\
                        connect(method="odbc",
                                  externalDSN=externalDsn,
                                  system=conn.host,
                                  username=conn.login,
                                  password=conn.password,
                                  charset='UTF8',
                                  );
        return conn

    def get_records(self, sql, parameters=None):
        """
        Executes the sql and returns a set of records.

        :param sql: the sql statement to be executed (str) or a list of
            sql statements to execute
        :type sql: str or list
        :param parameters: The parameters to render the SQL query with.
        :type parameters: mapping or iterable
        """
        if sys.version_info[0] < 3:
            sql = sql.encode('utf-8')
        conn = self.get_conn()
        cur = conn.cursor()
        if parameters is not None:
            cur.execute(sql, parameters)
        else:
            cur.execute(sql)
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return rows

    def insert_rows(self, table, rows, commit_every=1000):
        """
                A generic way to insert a set of tuples into a table,
                the whole set of inserts is treated as one transaction

                :param table: Name of the target table
                :type table: str
                :param rows: The rows to insert into the table
                :type rows: iterable of tuples
                :param target_fields: The names of the columns to fill in the table
                :type target_fields: iterable of strings
                :param commit_every: The maximum number of rows to insert in one
                    transaction. Set to 0 to insert all rows in one transaction.
                :type commit_every: int
                """
        conn = self.get_conn()
        cur = conn.cursor()
        i = 0
        for row in rows:
            i += 1
            l = []
            for cell in row:
                l.append(cell)
            values = values = ",".join(['?' for cell in range(0, len(row))])
            sql = "INSERT INTO {0} VALUES ({1});".format(
                table,
                values)
            cur.execute(sql, l)
            if commit_every and i % commit_every == 0:
                conn.commit()
                logging.info(
                    "Loaded {i} into {table} rows so far".format(**locals()))
        cur.close()
        conn.close()
        logging.info(
            "Done loading. Loaded a total of {i} rows".format(**locals()))

    def bulk_insert_rows(self, table, rows, commit_every=5000):
        import time
        start_time = time.time()
        """A performant bulk insert for Teradata that uses prepared statements via `executemany()`.
        For best performance, pass in `rows` as an iterator.
        """
        conn = self.get_conn()
        cursor = conn.cursor()
        logging.info("Starting batch insert...")
        values = ",".join(['?' for row in range(0, len(rows[0]))])
        prepared_stm = """INSERT INTO {0} VALUES ({1})""".format(
            table,
            values)
        logging.info(prepared_stm)
        row_count = 0
        # Chunk the rows
        row_chunk = []
        for row in rows:
            row_chunk.append(row)
            row_count += 1
            if row_count % commit_every == 0:
                cursor.executemany(prepared_stm, row_chunk, batch=True)
                #conn.commit()
                logging.info('[%s] inserted %s rows', table, row_count)
                # Empty chunk
                row_chunk = []
        # Commit the leftover chunk
        if len(row_chunk) > 0:
            cursor.executemany(prepared_stm, row_chunk, batch=True)
            logging.info('[%s] inserted %s rows', table, row_count)
        logging.info("Done loading. Loaded a total of " + str(len(rows)) + " rows in " + str(round(time.time() - start_time, 2)) + " second(s)")
        cursor.close()
        conn.close()