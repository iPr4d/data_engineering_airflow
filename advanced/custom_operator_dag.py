from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.datascientest_plugin import MySQLToMongoOperator

# Utils
def transform_date(today):
    hour_min = today.hour
    day = today.day
    if hour_min == 23:
        return [today.replace(hour=hour_min, minute=0, second=0),
                today.replace(day=day+1, hour=0, minute=0, second=0)]
    return [today.replace(hour=hour_min, minute=0, second=0),
                today.replace(hour=hour_min+1, minute=0, second=0)]

list_dates = transform_date(datetime.utcnow())

cities_ = Variable.get('cities').split(',')
cities = [x.encode('utf-8') for x in cities_]

sql_queries = ['SELECT AVG(temp_live), city, AVG(pressure), AVG(wind_speed)'
               ' from cities_live WHERE city = "{city}" AND '
               'time >= "{date_min}" AND time <= "{date_max}"'
               .format(city=city, date_min=str(list_dates[0]), date_max=str(list_dates[1]))
               for city in cities]

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
          schedule_interval=timedelta(hours=1))

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
