from datetime import datetime, timedelta
import random

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator

# DAG instantiation
default_args = {
    'owner': 'datascientest',
    'depends_on_past': False,
    'start_date': datetime.now()-timedelta(minutes=1, seconds=10),
    'retries': 1,
    'retry_delay': timedelta(seconds=15),
}

dag = DAG(dag_id='branching_dag',
          default_args=default_args,
          schedule_interval=timedelta(minutes=1))

# Python callable definition for BranchPythonOperator task

def branching_callable():
    return random.choice(['task_2_a', 'task_2_b'])

# Tasks definition

task_1 = DummyOperator(task_id='task_1',
                       dag=dag)

branching_task = BranchPythonOperator(task_id='branching_task',
                                      python_callable=branching_callable,
                                      dag=dag)

task_2_a = DummyOperator(task_id='task_2_a',
                         dag=dag)

task_2_b = DummyOperator(task_id='task_2_b',
                         dag=dag)

task_3_a = DummyOperator(task_id='task_3_a',
                         dag=dag)

task_3_b = DummyOperator(task_id='task_3_b',
                         dag=dag)

# Tasks dependencies

task_1 >> branching_task >> [task_2_a, task_2_b] 
task_2_a >> task_3_a
task_2_b >> task_3_b
