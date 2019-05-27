from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

# Setting default_args for DAG definition
default_args = {
    'owner': 'datascientest',
    'depends_on_past': False,
    # use a fixed datetime, this trick is just for presentation
    'start_date': datetime.now()-timedelta(minutes=5, seconds=10),
    'retries': 1,  # number of maximum retries if it fails
    'retry_delay': timedelta(seconds=15),  # time delay between two tries
    # 'end_date': datetime(2016, 1, 1),
    # 'email': ['airflow@example.com'],
    # 'email_on_failure': False,
    # 'email_on_retry': False,
}

# Instantiation of the DAG first_dag_datascientest
dag = DAG(dag_id='first_dag_datascientest',
          default_args=default_args,
          schedule_interval=timedelta(minutes=5))  # interval between two DAG runs

# Tasks definition using Operators

# First task definition : DummyOperator
start_dag = DummyOperator(task_id='start_dag',
                          dag=dag)

# Second task : BashOperator : we need to specify th bash command to execute
wait_before_printing = BashOperator(task_id='wait_before_printing',
                                    bash_command='sleep 5',
                                    dag=dag)

# Last task : print the execution_date with a PythonOperator

# Python callable definition


def print_date(ds, **kwargs):
    print("Execution date is :")
    print(kwargs['execution_date'])
    return ds


print_execution_date = PythonOperator(task_id='print_execution_date',
                                      python_callable=print_date,
                                      provide_context=True,
                                      dag=dag)

# Important note : here, print_date callable takes one parameter, and
# we do not specify any of this in PythonOperator python_callable parameter.
# This parameter ds is provided by the context (provide_context=True)
# You can see what's in the context by printing kwargs.

# Tasks dependencies

start_dag >> wait_before_printing >> print_execution_date
