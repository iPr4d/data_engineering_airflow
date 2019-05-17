from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from operators.datascientest_operators import MySQLToMySQLOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'datascientest',
    'depends_on_past': False,
    'start_date': datetime.now()-timedelta(minutes=15, seconds=10),
    'retries': 1,
    'retry_delay': timedelta(seconds=15),
}

dag = DAG(dag_id='custom_operator_dag',
          default_args=default_args,
          schedule_interval="@once")

start_dag = DummyOperator(task_id='start_dag',
                          dag=dag)

SELECT_NOT_RT = "SELECT * from trump WHERE text NOT LIKE 'RT%';"
SELECT_RT = "SELECT * from trump WHERE text LIKE 'RT%';"

extract_tweets = MySQLToMySQLOperator(
    sql_queries=[SELECT_NOT_RT, SELECT_RT],
    mysql_tables=['trump_rt', 'trump_not_rt'],
    src_mysql_conn_id='datascientest_sql_tweets',
    dest_mysql_conn_id='datascientest_sql_tweets_processed',
    task_id='extract_tweets',
    mysql_preoperator='create_tables.sql',
    dag=dag)

start_dag >> extract_tweets
