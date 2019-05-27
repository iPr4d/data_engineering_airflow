from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.hooks.mysql_hook import MySqlHook

from datetime import datetime, timedelta
import tweepy
from pymongo import MongoClient

# Utils

consumer_key = Variable.get('api_key')
consumer_secret = Variable.get('consumer_secret')
access_token = Variable.get('access_token')
access_token_secret = Variable.get('access_token_secret')

# subjects_ = Variable.get('subjects').split(',')
# subjects = [x.encode('utf-8') for x in subjects_]

subjects = ['trump', 'macron']

auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)

api = tweepy.API(auth)

mongo_client = MongoClient('localhost', 27017)
mongo_db = mongo_client.tweets

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


def get_tweets(**kwargs):

    # task instance kwargs op_kwargs
    subject = kwargs['subject']

    # getting data from Twitter API through python client
    list_tweets = []
    for tweet in tweepy.Cursor(api.search, q=subject).items(100):
        id = tweet.id_str
        created_at = str(tweet.created_at).encode('utf-8')
        text = tweet.text.encode('utf-8')
        author = tweet.author.screen_name.encode('utf-8')
        list_tweets += [{'id': id,
                         'text': text,
                         'date': created_at,
                         'author': author}]

    # automatically push data to XComs by returning result
    return list_tweets


def store_data_mysql(**kwargs):

    # task instance kwargs provided by the context and op_kwargs
    ti = kwargs['ti']
    subject = kwargs['subject']

    # get data from XComs
    list_tweets = ti.xcom_pull(key=None, task_ids='get_tweets_'+str(subject))

    # connect to MySQL server through Hook
    connection = MySqlHook(mysql_conn_id='datascientest_sql_tweets')

    sql_creation = 'CREATE TABLE IF NOT EXISTS {} (id varchar(255) primary ' \
                   'key, text varchar(1000), author varchar(255), '\
                   'date varchar(255))'.format(subject)

    connection.run(sql_creation)

    # adding tweets to MySQL subject table
    count = 0
    for tweet in list_tweets:
        id = tweet['id']
        text = tweet['text']
        author = tweet['author']
        date = tweet['date']
        sql_check_record = 'SELECT COUNT(*) from {subject} WHERE {subject}.id = {id}'.format(
            subject=subject, id=id)
        check_record_unique = connection.get_records(sql_check_record)[0][0]
        if check_record_unique == 0:
            sql_new_record = 'INSERT INTO {subject} (id, text, author, date) VALUES '.format(
                subject=subject)
            sql_new_record += '(%s,%s,%s,%s)'
            connection.run(sql_new_record, autocommit=True,
                           parameters=[id, text, author, date])
            count += 1
        else:
            print('Tweet {id} already stored in the DB'.format(id=id))
    return count


def remove_existing_rows(**kwargs):

    # task instance kwargs provided by the context and op_kwargs
    ti = kwargs['ti']
    subject = kwargs['subject']

    # get data from XComs
    list_tweets = ti.xcom_pull(key=None, task_ids='get_tweets_'+subject)

    # process data to removed existing tweets in db
    list_tweets_removed = [tweet for tweet in list_tweets if mongo_db[subject].find(
        {'id': tweet['id']}).count() == 0]

    # push processed data to XComs
    ti.xcom_push(key='cleaned_data', value=list_tweets_removed)

    return len(list_tweets)-len(list_tweets_removed)


def store_data_mongo(**kwargs):

    # task instance kwargs provided by the context and op_kwargs
    ti = kwargs['ti']
    subject = kwargs['subject']

    # get data from XComs
    list_tweets_to_add = ti.xcom_pull(
        key='cleaned_data', task_ids='remove_existing_rows_'+subject)

    # process data
    collection = mongo_db[subject]
    result = collection.insert_many(list_tweets_to_add)

    return len(result.inserted_ids)


def check_inserted_values(**kwargs):
    # task instance kwargs provided by the context and op_kwargs
    ti = kwargs['ti']

    # get results from XComs
    for subject in subjects:
        my_sql_inserted_count = ti.xcom_pull(
            key='return_value', task_ids='store_data_mysql_'+subject)
        mongo_inserted_count = ti.xcom_pull(
            key='return_value', task_ids='store_data_mongo_'+subject)
        if my_sql_inserted_count != mongo_inserted_count:
            raise ValueError(
                'Inserted values are not the same in both services MySQL and MongoDB')

# Tasks definition


start_dag = DummyOperator(task_id='start_dag',
                          dag=dag)

mysql_done = DummyOperator(task_id='mysql_done',
                           dag=dag)

mongo_done = DummyOperator(task_id='mongo_done',
                           dag=dag)

check_inserted_values_operator = PythonOperator(task_id='check_inserted_values',
                                                provide_context=True,
                                                python_callable=check_inserted_values,
                                                dag=dag)

for subject in subjects:

    store_data_mysql_operator = PythonOperator(task_id='store_data_mysql_'+subject.replace(' ', '_'),
                                               provide_context=True,
                                               python_callable=store_data_mysql,
                                               op_kwargs={
                                                   'subject': subject.replace(' ', '_')},
                                               dag=dag)

    store_data_mongo_operator = PythonOperator(task_id='store_data_mongo_'+subject.replace(' ', '_'),
                                               provide_context=True,
                                               python_callable=store_data_mongo,
                                               op_kwargs={
                                                   'subject': subject.replace(' ', '_')},
                                               dag=dag)

    remove_existing_rows_operator = PythonOperator(task_id='remove_existing_rows_'+subject.replace(' ', '_'),
                                                   provide_context=True,
                                                   python_callable=remove_existing_rows,
                                                   op_kwargs={
                                                       'subject': subject.replace(' ', '_')},
                                                   dag=dag)

    get_tweets_operator = PythonOperator(task_id='get_tweets_'+subject.replace(' ', '_'),
                                         python_callable=get_tweets,
                                         op_kwargs={'subject': subject},
                                         dag=dag)

    # Set dependencies between tasks

    start_dag >> get_tweets_operator
    get_tweets_operator >> [store_data_mysql_operator,
                            remove_existing_rows_operator]
    remove_existing_rows_operator >> store_data_mongo_operator
    mysql_done.set_upstream(store_data_mysql_operator)
    mongo_done.set_upstream(store_data_mongo_operator)
    check_inserted_values_operator.set_upstream([mysql_done, mongo_done])
