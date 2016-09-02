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


class TeradataHook(DbApiHook):
    """
    Interact with Teradata SQL.
    """
    conn_name_attr = 'teradata_conn_id'
    default_conn_name = 'teradata_default'
    supports_autocommit = False

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
        dsn = conn.extra_dejson.get('dsn', None)
        appn = conn.extra_dejson.get('appName', 'airflow')
        ver = conn.extra_dejson.get('version', '1.0')
        log = conn.extra_dejson.get('logConsole', False)

        udaExec = teradata.UdaExec(appName=appn,
                                   version=ver,
                                   logConsole=log )

        session = udaExec.connect(method="odbc",
                                  externalDSN=dsn,
                                  system=conn.host,
                                  username=conn.login,
                                  password=conn.password,
                                  charset='UTF8');
        return session

