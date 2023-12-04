import logging
import os
import sys
from datetime import timedelta

sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from airflow import DAG
from airflow.utils.dates import days_ago
from common.base import params, session


ENV = os.environ.get("ENVIRONMENT")

dag_id = 'python_callback'


def test_callback(**kwargs):
        env = kwargs['ENV']
        print(env)
        date = kwargs['LAST_EXECUTION_DATE']
        print(date)
        logging.info(env)
        logging.info(date)

def get_data_form_athena():
    try:
        logger.debug(f'Get data from athena')
        query = get_query(SCHEMA)
        print(query)
        logging.info(query)
        query_executor = Athena.execute(query, params, session)
        query_results_df = next(query_executor)
        list = []
        for index, row in dealer_query_results_df.iterrows():
            print(row)
            logging.info(row)
        return
    except ConnectionError as e:
        logger.debug(f"ConnectionError : {str(e)}")
    except Exception as e:
        logger.debug(f"Exception : {str(e)}")

def get_query(schema):
    return \
        f""" SELECT *
            FROM {schema}.core_events
            limit 10
        """

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
                                      "ENV": ENV,
                                      "LAST_EXECUTION_DATE" : "{{ prev_execution_date_success.int_timestamp }}"
                                  })

# task1 = PythonOperator(task_id='test_callback',
#                                   provide_context=True,
#                                   python_callable=get_data_form_athena,
#                                   dag=dag
#                                   )
#
# task >> task1

globals()[dag_id] = dag