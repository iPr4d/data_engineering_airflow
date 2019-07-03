# -*- coding: utf-8 -*-

import logging
from datetime import datetime

import MySQLdb
from airflow.contrib.hooks.mongo_hook import MongoHook
from airflow.hooks.mysql_hook import MySqlHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class MySQLToMongoOperator(BaseOperator):
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

    template_fields = ('sql_queries', 'parameters', 'mysql_preoperator')
    template_ext = ('.sql',)
    ui_color = '#a87abc'

    @apply_defaults
    def __init__(
            self,
            sql_queries,
            mongo_collections,
            mysql_conn_id='mysql_default',
            mongo_conn_id='mongo_default',
            mysql_preoperator=None,
            parameters=None,
            *args, **kwargs):
        super(MySQLToMongoOperator, self).__init__(*args, **kwargs)
        self.sql_queries = sql_queries
        self.mongo_collections = mongo_collections
        self.mysql_conn_id = mysql_conn_id
        self.mongo_conn_id = mongo_conn_id
        self.mysql_preoperator = mysql_preoperator
        self.parameters = parameters

    def execute(self, context):
        logging.info('Executing: ' + str(self.sql_queries))
        mysql_hook = MySqlHook(mysql_conn_id=self.mysql_conn_id)
        mongo_hook = MongoHook(mongo_conn_id=self.mongo_conn_id)

        logging.info(
            "Transferring MySQL query results into MongoDB database.")

        mysql_conn = mysql_hook.get_conn()
        mysql_conn.cursorclass = MySQLdb.cursors.DictCursor
        cursor = mysql_conn.cursor()

        mongo_conn = mongo_hook.get_conn()
        mongo_db = mongo_conn.weather

        if self.mysql_preoperator:
            logging.info("Running MySQL preoperator")
            cursor.execute(self.mysql_preoperator)

        for index, sql in enumerate(self.sql_queries):
            cursor.execute(sql, self.parameters)

            fetched_rows = list(cursor.fetchall())

            mongo_db[self.mongo_collections[index]].insert_many(fetched_rows)

        logging.info("Transfer Done")
