from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.hooks.mysql_hook import MySqlHook

from datetime import datetime, timedelta
import tweepy
from pymongo import MongoClient

# Utils

consumer_key = Variable.get('consumer_key')
consumer_secret = Variable.get('consumer_secret')
access_token = Variable.get('access_token')
access_token_secret = Variable.get('access_token_secret')

subjects_ = Variable.get('subjects').split(',')
subjects = [x.encode('utf-8') for x in subjects_]

auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)

api = tweepy.API(auth)

mongo_client = MongoClient('localhost', 27017)
mongo_db = mongo_client.tweets

# DAG instanciation 

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

dag = DAG('variables_xcoms_hooks', default_args=default_args, schedule_interval=timedelta(minutes=15))

# Python callables definition for PythonOperator tasks

def get_tweets(**kwargs):
    subject = kwargs['subject']
    list_tweets = []
    for tweet in tweepy.Cursor(api.search, q=subject).items(100):
        id = tweet.id_str
        created_at = str(tweet.created_at).encode('utf-8')
        text = tweet.text.encode('utf-8')
        author = tweet.author.screen_name.encode('utf-8')
        list_tweets += [{'id': id, 'text': text, 'date': created_at, 'author': author}]
    return list_tweets


def store_data_mysql(**kwargs):
    ti = kwargs['ti']
    subject = kwargs['subject']
    list_tweets = ti.xcom_pull(key=None, task_ids='get_tweets_'+str(subject))
    connection = MySqlHook(mysql_conn_id='datascientest_sql_connexion')
    sql_creation = 'CREATE TABLE IF NOT EXISTS {} (id varchar(255) primary key, text varchar(1000), author varchar(255), date varchar(255))'.format(subject)

    connection.run(sql_creation)
    count = 0 
    for tweet in list_tweets:
        id = tweet['id']
        text = tweet['text']
        author = tweet['author']
        date = tweet['date']
        sql_check_record = 'SELECT COUNT(*) from {subject} WHERE {subject}.id = {id}'.format(subject=subject, id=id)
        check_record_unique = connection.get_records(sql_check_record)[0][0]
        if check_record_unique == 0:
            sql_new_record = 'INSERT INTO {subject} (id, text, author, date) VALUES '.format(subject=subject)
            sql_new_record += '(%s,%s,%s,%s)'
            connection.run(sql_new_record, autocommit=True, parameters=[id, text, author, date])
            count += 1
        else:
            print('Tweet {id} already stored in the DB'.format(id=id))
    return '{count} rows inserted'.format(count=count)


def remove_existing_rows(**kwargs):
    ti = kwargs['ti']
    subject = kwargs['subject']
    list_tweets = ti.xcom_pull(key=None, task_ids='get_tweets_'+subject)
    list_tweets_removed = [tweet for tweet in list_tweets if mongo_db[subject].find({ 'id': tweet['id'] }).count() == 0]
    print('{count} removed rows'.format(count=len(list_tweets)-len(list_tweets_removed)))
    return list_tweets_removed

def store_data_mongo(**kwargs):
    ti = kwargs['ti']
    subject = kwargs['subject']
    list_tweets = ti.xcom_pull(key=None, task_ids='remove_existing_rows_'+subject)
    collection = mongo_db[subject]
    result = collection.insert_many(list_tweets)
    return '{count} rows inserted'.format(count=len(result.inserted_ids))

# Tasks definition

start_dag = DummyOperator(task_id='start_dag',
                          dag=dag)

for subject in subjects :

    store_data_mysql_operator = PythonOperator(task_id='store_data_mysql_'+subject,
                                               provide_context=True,
                                               python_callable=store_data_mysql,
                                               op_kwargs={'subject': subject},
                                               dag=dag)

    store_data_mongo_operator = PythonOperator(task_id='store_data_mongo_'+subject,
                                               provide_context=True,
                                               python_callable=store_data_mongo,
                                               op_kwargs={'subject': subject},
                                               dag=dag)

    remove_existing_rows_operator = PythonOperator(task_id='remove_existing_rows_'+subject,
                                               provide_context=True,
                                               python_callable=remove_existing_rows,
                                               op_kwargs={'subject': subject},
                                               dag=dag)

    get_tweets_operator = PythonOperator(task_id='get_tweets_'+subject,
                                         python_callable=get_tweets,
                                         op_kwargs={'subject': subject},
                                         dag=dag)

    # Set dependencies between tasks

    start_dag >> get_tweets_operator
    get_tweets_operator >> [store_data_mysql_operator, remove_existing_rows_operator]
    remove_existing_rows_operator >> store_data_mongo_operator

