import logging
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from airflow import DAG


ENV = os.environ.get("ENVIRONMENT")

dag_id = 'python_callback'


def test_callback(entity_type, tag_name):
        env = kwargs['ENV']
        print(env)
        logging.info(env)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': f'{START_DATE}',
    "on_failure_callback": email_template.notify_email,
    'ENV': ENV
}

dag = DAG(
        dag_id, default_args=default_args, is_paused_upon_creation=False, schedule_interval='0 5 * * *', catchup=False,
        tags=['Python-test'])

task = PythonOperator(task_id='send_activation_notification',
                                  provide_context=True,
                                  python_callable=send_activation_notification_bo,
                                  dag=dag,
                                  op_kwargs={
                                      "ENV": ENV
                                  })

globals()[dag_id] = dag