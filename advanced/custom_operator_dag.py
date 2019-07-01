from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from operators.datascientest_operators import MySQLToMongoOperator

# Utils
def transform_date(today):
    day = str(today.day)
    month = str(today.month)
    year = str(today.year)
    if len(day) == 1:
        day = '0'+day
    if len(month) == 1:
        month = '0'+month
    return year + '-' + month + '-' + day

sql_queries = ['SELECT AVG(temp_live), city, AVG(pressure), AVG(wind_speed)'
               ' from cities_live WHERE city = "{city}" AND '
               'SUBSTRING(time, 1, 10) = {today}'
               .format(city=city, today=transform_date(datetime.today()))
               for city in cities]

cities_ = Variable.get('cities').split(',')
cities = [x.encode('utf-8') for x in cities_]

# DAG instantiation
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

# Tasks definition

start_dag = DummyOperator(task_id='start_dag',
                          dag=dag)


transfer_weather_data = MySQLToMongoOperator(
    sql_queries=sql_queries,
    mongo_collections=cities,
    mysql_conn_id='datascientest_sql_weather',
    mongo_conn_id='datascientest_mongo_weather',
    task_id='extract_weather_data',
    dag=dag)

# Tasks dependencies

start_dag >> transfer_weather_data
