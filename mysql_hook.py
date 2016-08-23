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

import MySQLdb
import MySQLdb.cursors
import logging
from airflow.hooks.dbapi_hook import DbApiHook


class MySqlHook(DbApiHook):
    '''
    Interact with MySQL.
    You can specify charset in the extra field of your connection
    as ``{"charset": "utf8"}``. Also you can choose cursor as
    ``{"cursor": "SSCursor"}``. Refer to the MySQLdb.cursors for more details.
    '''

    conn_name_attr = 'mysql_conn_id'
    default_conn_name = 'mysql_default'
    supports_autocommit = True

    def get_conn(self):
        """
        Returns a mysql connection object
        """
        conn = self.get_connection(self.mysql_conn_id)
        conn_config = {
            "user": conn.login,
            "passwd": conn.password or ''
        }

        conn_config["host"] = conn.host or 'localhost'
        if not conn.port:
            conn_config["port"] = 3306
        else:
            conn_config["port"] = int(conn.port)

        conn_config["db"] = conn.schema or ''

        if conn.extra_dejson.get('charset', False):
            conn_config["charset"] = conn.extra_dejson["charset"]
            if (conn_config["charset"]).lower() == 'utf8' or\
                    (conn_config["charset"]).lower() == 'utf-8':
                conn_config["use_unicode"] = True
        if conn.extra_dejson.get('cursor', False):
            if (conn.extra_dejson["cursor"]).lower() == 'sscursor':
                conn_config["cursorclass"] = MySQLdb.cursors.SSCursor
            elif (conn.extra_dejson["cursor"]).lower() == 'dictcursor':
                conn_config["cursorclass"] = MySQLdb.cursors.DictCursor
            elif (conn.extra_dejson["cursor"]).lower() == 'ssdictcursor':
                conn_config["cursorclass"] = MySQLdb.cursors.SSDictCursor
        local_infile = conn.extra_dejson.get('local_infile',False)
        if conn.extra_dejson.get('ssl', False):
            conn_config['ssl'] = conn.extra_dejson['ssl']
        if local_infile:
            conn_config["local_infile"] = 1
        conn = MySQLdb.connect(**conn_config)
        return conn

    def bulk_load(self, table, tmp_file):
        """
        Loads a tab-delimited file into a database table
        """
        conn = self.get_conn()
        cur = conn.cursor()
        cur.execute("""
            LOAD DATA LOCAL INFILE '{tmp_file}'
            INTO TABLE {table}
            """.format(**locals()))
        conn.commit()

    def bulk_load_ib(self, table, database, tmp_file, rel_path='/opt', sep='\t', header = 0, ftp_conn_id='ftp_default'):
        import uuid
        from airflow.contrib.hooks.ftp_hook import FTPHook

        """
        Loads a tab-delimited file into a database table using FTPHook for transfering
        CSV file since ICE don't support LOAD DATA LOCAL INFILE. Doesn't support IGNORE X LINES, NO HEADER

        # TODO : DELETE HEADER OPTION
        """

        logging.info('Load file to table : {}'.format(table))
        logging.info('Using Database: {}'.format(database))
        conn_FTP = FTPHook(ftp_conn_id=ftp_conn_id)
        # Remote temp file name and dir
        tmp_file_remote = database + '_' + table + '.csv'
	rnd = str(uuid.uuid4())
        tmp_dir_remote = '/ibl' + rnd + '/'
        conn_FTP.create_directory(tmp_dir_remote)
        logging.info('Temp folder created : {}'.format(tmp_dir_remote))
        temp_filepath = tmp_dir_remote + tmp_file_remote
        logging.info('Transfering file : {}'.format(temp_filepath))
        remote_filepath = rel_path + temp_filepath
        logging.info('Remote file : {}'.format(remote_filepath))
        try:
            conn_FTP.store_file(temp_filepath, tmp_file)
        except:
            logging.warning("Failed to store file")
            conn_FTP.delete_directory(tmp_dir_remote)
            raise
            # Load Remote temp file uploaded to Infobright Server
        try:
            conn = self.get_conn()
            logging.info('Loading data to Infobright...')
            cur = conn.cursor()

            cur.execute("""
                set @bh_dataformat = 'txt_variable'
            """.format(**locals()))

            cur.execute("""
                set @BH_REJECT_FILE_PATH = '/opt/{rnd}_{database}_{table}_reject.txt'
            """.format(**locals()))

            cur.execute("""
                LOAD DATA INFILE '{remote_filepath}'
                INTO TABLE {database}.{table}
                FIELDS TERMINATED BY '{sep}'
                """.format(**locals()))
            conn.commit()
            logging.info('Finished loading data')
        except:
            logging.warning("Failed to execute SQL Statement")
            conn_FTP.delete_file(temp_filepath)
            conn_FTP.delete_directory(tmp_dir_remote)
            raise
        # Remove temp dir after commit
        conn_FTP.delete_file(temp_filepath)
        conn_FTP.delete_directory(tmp_dir_remote)
        logging.info('Removed temp folder')
