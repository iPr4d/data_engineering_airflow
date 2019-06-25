import json
from datetime import datetime, timedelta

import requests
from airflow import DAG
from airflow.hooks.mysql_hook import MySqlHook
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

# Utils
api_key = Variable.get('api_key')
cities_ = Variable.get('cities').split(',')
cities = [x.encode('utf-8') for x in cities_]

def open_weather_response_parser(response_text, city):
    response_dict = json.loads(response_text)
    main = response_dict['main']
    temp_live = main['temp'] - 273.15
    temp_max = main['temp_max'] - 273.15
    temp_min = main['temp_min'] - 273.15
    humidity = main['humidity']
    pressure = main['pressure']
    weather = response_dict['weather'][0]['main']
    wind_speed = response_dict['wind']['speed']
    time = datetime.utcnow()
    parsed_response = {'city': city,
                       'temp_live': temp_live,
                       'temp_max': temp_max,
                       'temp_min': temp_min,
                       'humidity': humidity,
                       'pressure': pressure,
                       'weather_description': weather,
                       'wind_speed': wind_speed,
                       'time': str(time)}
    return parsed_response


# DAG instantiation
default_args = {
    'owner': 'datascientest',
    'depends_on_past': False,
    'start_date': datetime.now()-timedelta(minutes=15, seconds=10),
    'retries': 1,
    'retry_delay': timedelta(seconds=15),
    # 'end_date': datetime(2016, 1, 1),
    # 'email': ['airflow@example.com'],
    # 'email_on_failure': False,
    # 'email_on_retry': False,
}

dag = DAG(dag_id='variables_xcoms_hooks',
          default_args=default_args,
          schedule_interval=timedelta(minutes=15))

# Python callables definition for PythonOperator tasks
def get_weather(**kwargs):

    # task instance kwargs op_kwargs
    city = kwargs['city']

    # getting data from OpenWeather API using requests python package
    url_endpoint = 'http://api.openweathermap.org/data/2.5/weather?q={city}&APPID={api_key}' \
                    .format(city=city, api_key=api_key)

    response = requests.get(url_endpoint)

    # automatically push data to XComs by returning result if request status is OK
    if response.status_code == 200:
        return open_weather_response_parser(response.text, city)
    else:
        return

def store_data_mysql(**kwargs):

    # task instance kwargs provided by the context and op_kwargs
    ti = kwargs['ti']

    # get data from XComs
    list_new_weather = [ti.xcom_pull(key=None, task_ids='get_weather_'+str(city).replace(' ', '')) for city in cities]

    # connect to MySQL server (and database!) through MySqlHook
    connection = MySqlHook(mysql_conn_id='datascientest_sql_weather')

    # create table to store weather data if not exists
    sql_creation = 'CREATE TABLE IF NOT EXISTS cities_live (id int primary ' \
                   'key not null auto_increment, city varchar(1000), temp_live float, ' \
                   'temp_min float, temp_max float, humidity float, pressure float, weather_description varchar(1000), ' \
                   'wind_speed float, time varchar(255))'
    connection.run(sql_creation)

    # adding new weather results to MySQL weather table
    for new_weather in list_new_weather:
        sql_new_record = 'INSERT INTO cities_live (city, temp_live, temp_min, temp_max, humidity, pressure, weather_description, wind_speed, time) VALUES '
        sql_new_record += '(%s,%s,%s,%s,%s,%s,%s,%s,%s)'
        parameters = [new_weather['city'], new_weather['temp_live'], new_weather['temp_min'], new_weather['temp_max'], new_weather['humidity'],
                      new_weather['pressure'], new_weather['weather_description'], new_weather['wind_speed'], new_weather['time']]
        connection.run(sql_new_record, 
                       autocommit=True,
                       parameters=parameters)

# Tasks definition
start_dag = DummyOperator(task_id='start_dag',
                          dag=dag)

store_data_mysql_operator = PythonOperator(task_id='store_data_mysql',
                                               provide_context=True,
                                               python_callable=store_data_mysql,
                                               dag=dag)

for city in cities:
    get_weather_operator = PythonOperator(task_id='get_weather_'+city.replace(' ', ''),
                                         python_callable=get_weather,
                                         op_kwargs={'city': city},
                                         dag=dag)

    # Set dependencies between tasks
    start_dag >> get_weather_operator
    get_weather_operator >> store_data_mysql_operator
