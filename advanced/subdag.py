from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.subdag_operator import SubDagOperator


# Utils
start_date = datetime.now()-timedelta(minutes=15, seconds=10)
schedule_interval = "@once"

# DAG instantiation
default_args = {
    'owner': 'datascientest',
    'depends_on_past': False,
    'start_date': start_date,
    'retries': 1,
    'retry_delay': timedelta(seconds=15),
}

dag = DAG(dag_id='subdag_dag',
          default_args=default_args,
          schedule_interval=schedule_interval)

# SubDAG instantiation and tasks definition

subdag = DAG(dag_id='subdag_dag.subdag',
          default_args=default_args, # Should have the same arguments here
          schedule_interval=schedule_interval) # Same here

start_subdag = DummyOperator(task_id='start_subdag', dag=subdag)
end_subdag = DummyOperator(task_id='end_subdag', dag=subdag)

start_subdag >> end_subdag

# Tasks definition

start_task = DummyOperator(task_id='start_dag',
                           dag=dag)

end_task = DummyOperator(task_id='end_dag',
                         dag=dag)

sub_dag_task = SubDagOperator(dag=dag,
                              subdag=subdag,
                              task_id='subdag')

# Tasks dependencies

start_task >> sub_dag_task >> end_task
