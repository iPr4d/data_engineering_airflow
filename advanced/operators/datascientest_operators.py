# -*- coding: utf-8 -*-

import logging
from datetime import datetime

from airflow.hooks.mysql_hook import MySqlHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class MySQLToMySQLOperator(BaseOperator):
    """
    Executes sql code in a MySQL database and inserts into another
    :param src_mysql_conn_id: reference to the source MySQL database
    :type src_mysql_conn_id: string
    :param dest_mysql_conn_id: reference to the destination MySQL database
    :type dest_mysql_conn_id: string
    :param sql_queries: list of sql code to be executed
    :type sql_queries: Can receive a List of str representing a sql statement,
        a list of str (sql statements), or reference to a template file.
        Template reference are recognized by str ending in '.sql'
    :param parameters: a parameters dict that is substituted at query runtime.
    :type parameters: dict
    """

    template_fields = ('sql_queries', 'parameters', 'mysql_tables',
                       'mysql_preoperator', 'mysql_postoperator')
    template_ext = ('.sql',)
    ui_color = '#a87abc'

    @apply_defaults
    def __init__(
            self,
            sql_queries,
            mysql_tables,
            src_mysql_conn_id='mysql_default',
            dest_mysql_conn_id='mysql_default',
            mysql_preoperator=None,
            mysql_postoperator=None,
            parameters=None,
            *args, **kwargs):
        super(MySQLToMySQLOperator, self).__init__(*args, **kwargs)
        self.sql_queries = sql_queries
        self.mysql_tables = mysql_tables
        self.src_mysql_conn_id = src_mysql_conn_id
        self.dest_mysql_conn_id = dest_mysql_conn_id
        self.mysql_preoperator = mysql_preoperator
        self.mysql_postoperator = mysql_postoperator
        self.parameters = parameters

    def execute(self, context):
        logging.info('Executing: ' + str(self.sql_queries))
        src_mysql = MySqlHook(mysql_conn_id=self.src_mysql_conn_id)
        dest_mysql = MySqlHook(mysql_conn_id=self.dest_mysql_conn_id)

        logging.info(
            "Transferring MySQL query results into other MySQL database.")
        conn = src_mysql.get_conn()
        cursor = conn.cursor()

        if self.mysql_preoperator:
            logging.info("Running MySQL preoperator")
            dest_mysql.run(self.mysql_preoperator)

        for index, sql in enumerate(self.sql_queries):
            cursor.execute(sql, self.parameters)

            logging.info("Inserting rows into MySQL table {name}".format(
                name=self.mysql_tables[index]))

            dest_mysql.insert_rows(table=self.mysql_tables[index], rows=cursor)

            if self.mysql_postoperator:
                logging.info("Running MySQL postoperator")
                dest_mysql.run(self.mysql_postoperator)

        logging.info("Transfer Done")
