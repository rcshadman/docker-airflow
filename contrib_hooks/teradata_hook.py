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
from past.builtins import basestring
from datetime import datetime
import numpy
import logging
import sys
import re


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


        teradata_log = logging.getLogger("teradata")
        teradata_log.addHandler(logging.NullHandler())
        teradata_log.propagate = False

        conn = teradata.UdaExec(appName=appn,
                                version=ver,
                                logConsole=log,
                                configureLogging=log,
                                logFile=None,
                                logLevel="ERROR",
                                checkpointFile=False
                                ).\
                        connect(method="odbc",
                                  externalDSN=externalDsn,
                                  system=conn.host,
                                  username=conn.login,
                                  password=conn.password,
                                  charset='UTF8',
                                  transactionMode='Teradata',
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

    def run(self, sql, autocommit=False, parameters=None):
        """
        Runs a command or a list of commands. Pass a list of sql
        statements to the sql parameter to get them to execute
        sequentially

        :param sql: the sql statement to be executed (str) or a list of
            sql statements to execute
        :type sql: str or list
        :param autocommit: What to set the connection's autocommit setting to
            before executing the query.
        :type autocommit: bool
        :param parameters: The parameters to render the SQL query with.
        :type parameters: mapping or iterable
        """
        conn = self.get_conn()
        if isinstance(sql, basestring):
            sql = [sql]

        if self.supports_autocommit:
            self.set_autocommit(conn, autocommit)

        cur = conn.cursor()
        for s in self.sqlsplit(sql):
            if sys.version_info[0] < 3:
                s = s.encode('utf-8')
            logging.info(s)
            if parameters is not None:
                cur.execute(s, parameters)
            else:
                cur.execute(s)
        cur.close()
        conn.commit()
        conn.close()

    def insert_rows(self, table, rows, commit_every=1000, unicode_source=True):
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
        # Workaround when source is unicode
        self.unicode_source = unicode_source
        conn = self.get_conn()
        cur = conn.cursor()
        i = 0
        for row in rows:
            i += 1
            l = []
            for cell in row:
                l.append(self.serialize_cell(cell))
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

    def bulk_insert_rows(self, table, rows, commit_every=5000, unicode_source=True):
        """A performant bulk insert for Teradata that uses prepared statements via `executemany()`.
        For best performance, pass in `rows` as an iterator.
        """
        #Workaround when source is unicode
        self.unicode_source = unicode_source
        conn = self.get_conn()
        cursor = conn.cursor()
        row_count = 0
        # Chunk the rows
        row_chunk = []
        for row in rows:
            if row_count == 0:
                values = ",".join(['?' for cell in range(0, len(row))])
                prepared_stm = """INSERT INTO {0} VALUES ({1})""".format(
                    table,
                    values)
            serialized_row = []
            for cell in row:
                serialized_row.append(self.serialize_cell(cell))
            row_chunk.append(tuple(serialized_row))
            row_count += 1
            if row_count % commit_every == 0:
                cursor.executemany(prepared_stm, row_chunk, batch=True)
                logging.info('Loaded %s into %s rows so far', row_count, table)
                # Empty chunk
                row_chunk = []
        # Commit the leftover chunk
        if len(row_chunk) > 0:
            cursor.executemany(prepared_stm, row_chunk, batch=True)
            logging.info('Loaded %s into %s rows so far', row_count, table)
        logging.info("Done loading. Loaded a total of {} rows".format(row_count))
        cursor.close()
        conn.close()

    def serialize_cell(self, cell):
        if isinstance(cell, basestring):
            #TODO: Fix this
            if self.unicode_source:
                return unicode(cell)
            else:
                return unicode(cell.decode('latin1'))#This assumes that input is in latin1
        elif cell is None:
            return None
        elif isinstance(cell, numpy.datetime64):
            return unicode(str(cell))
        elif isinstance(cell, datetime):
            return unicode(cell.isoformat())
        else:
            return unicode(str(cell))

    def isString(self, value):
        # Implement python version specific setup.
        if sys.version_info[0] == 2:
            return isinstance(value, basestring)  # @UndefinedVariable
        else:
            return isinstance(value, str)  # @UndefinedVariable

    def sqlsplit(self, sql, delimiter=";"):
        """A generator function for splitting out SQL statements according to the
         specified delimiter. Ignores delimiter when in strings or comments."""
        tokens = re.split("(--|'|\n|" + re.escape(delimiter) + "|\"|/\*|\*/)",
                          sql if self.isString(sql) else delimiter.join(sql))
        statement = []
        inComment = False
        inLineComment = False
        inString = False
        inQuote = False
        for t in tokens:
            if not t:
                continue
            if inComment:
                if t == "*/":
                    inComment = False
            elif inLineComment:
                if t == "\n":
                    inLineComment = False
            elif inString:
                if t == '"':
                    inString = False
            elif inQuote:
                if t == "'":
                    inQuote = False
            elif t == delimiter:
                sql = "".join(statement).strip()
                if sql:
                    yield sql
                statement = []
                continue
            elif t == "'":
                inQuote = True
            elif t == '"':
                inString = True
            elif t == "/*":
                inComment = True
            elif t == "--":
                inLineComment = True
            statement.append(t)
        sql = "".join(statement).strip()
        if sql:
            yield sql