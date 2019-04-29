from airflow import DAG
from airflow.models import Variable
from airflow.hooks.mysql_hook import MySqlHook
from pymongo import MongoClient
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import tweepy

consumer_key = Variable.get('consumer_key')
consumer_secret = Variable.get('consumer_secret')
access_token = Variable.get('access_token')
access_token_secret = Variable.get('access_token_secret')

auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)

api = tweepy.API(auth)

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


def get_tweets():
    list_tweets = []
    for tweet in tweepy.Cursor(api.search, q='trump').items(100):
        id = tweet.id_str
        created_at = str(tweet.created_at).encode('utf-8')
        text = tweet.text.encode('utf-8')
        author = tweet.author.screen_name.encode('utf-8')
        list_tweets += [{'id': id, 'text': text, 'date': created_at, 'author': author}]
    return list_tweets

def store_data_mysql(**kwargs):
    ti = kwargs['ti']
    list_tweets = ti.xcom_pull(key=None, task_ids='get_tweets')
    connection = MySqlHook(mysql_conn_id='datascientest_sql_connexion')
    connection.run('CREATE DATABASE IF NOT EXISTS tweets;')
    connection.run('USE tweets; CREATE TABLE IF NOT EXISTS trump (id varchar(255) primary key, text varchar(255), author varchar(255), date varchar(255))')
    for tweet in list_tweets:
        id = tweet['id']
        text = tweet['text']
        author = tweet['author']
        date = tweet['date']
        sql = 'USE tweets; INSERT INTO trump (id, text, author, date) VALUES (%s,%s,%s,%s)'
        try:
            connection.run(sql, autocommit=True, parameters=(id, text, author, date))
        except Exception as e:
            print(e)
            
            

def store_data_mongo(**kwargs):
    ti = kwargs['ti']
    list_tweets = ti.xcom_pull(key=None, task_ids='get_tweets')
    client = MongoClient('localhost', 27017)
    db = client.tweets
    trump_collection = db.trump
    result = trump_collection.insert_many(list_tweets)
    print(result.inserted_ids)
    print(len(result.inserted_ids))


store_data_mysql_operator = PythonOperator(task_id='store_data_mysql',
                                           provide_context=True,
                                           python_callable=store_data_mysql,
                                           dag=dag)

store_data_mongo_operator = PythonOperator(task_id='store_data_mongo',
                                           provide_context=True,
                                           python_callable=store_data_mongo,
                                           dag=dag)

get_tweets_operator = PythonOperator(task_id='get_tweets',
                                     python_callable=get_tweets,
                                     dag=dag)

get_tweets_operator >> [store_data_mysql_operator, store_data_mongo_operator]
