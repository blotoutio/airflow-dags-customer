import logging
import os
import sys
from datetime import timedelta

sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from airflow import DAG
from airflow.utils.dates import days_ago


ENV = os.environ.get("ENVIRONMENT")

dag_id = 'python_callback'


def test_callback(**kwargs):
        env = kwargs['ENV']
        print(env)
        logging.info(env)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': days_ago(1),
    'ENV': ENV
}

dag = DAG(
        dag_id, default_args=default_args, is_paused_upon_creation=False, schedule_interval='0 5 * * *', catchup=False,
        tags=['Python-test'])

task = PythonOperator(task_id='test_callback',
                                  provide_context=True,
                                  python_callable=test_callback,
                                  dag=dag,
                                  op_kwargs={
                                      "ENV": ENV
                                  })

globals()[dag_id] = dag