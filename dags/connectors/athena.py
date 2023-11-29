import re
import time

import boto3
import pandas as pd
from smart_open import smart_open

from connectors.constant import ATHENA_RESULTS_BATCH_SIZE


def connect(params):
    return boto3.Session(aws_access_key_id=params['AWS_ACCESS_KEY'],
                         aws_secret_access_key=params['AWS_SECRET_KEY'],
                         region_name=params['AWS_REGION'])


def execute(query, params, session):
    params['query'] = query
    s3_output_filename = athena_to_s3(session, params)
    return output_generator(s3_output_filename, params)


def trigger_query(client, params):
    response = client.start_query_execution(
        QueryString=params["query"],
        ResultConfiguration={
            'OutputLocation': params['path']
        }
    )
    return response


def athena_to_s3(session, params):
    client = session.client('athena', region_name=params["AWS_REGION"])
    execution = trigger_query(client, params)
    execution_id = execution['QueryExecutionId']
    state = 'RUNNING'

    while (state in ['RUNNING', 'QUEUED']):
        response = client.get_query_execution(QueryExecutionId=execution_id)

        if 'QueryExecution' in response and \
                'Status' in response['QueryExecution'] and \
                'State' in response['QueryExecution']['Status']:
            state = response['QueryExecution']['Status']['State']
            if state == 'FAILED':
                return False
            elif state == 'SUCCEEDED':
                s3_path = response['QueryExecution']['ResultConfiguration']['OutputLocation']
                filename = re.findall('.*\/(.*)', s3_path)[0]
                return filename
        time.sleep(1)

    return False


def output_generator(s3_output_filename, params):
    object_key = f"reverse-el/{s3_output_filename}"
    aws_key = params['AWS_ACCESS_KEY']
    aws_secret = params['AWS_SECRET_KEY']
    bucket_name = params['bucket']

    path = 's3://{}:{}@{}/{}'.format(aws_key, aws_secret, bucket_name, object_key)
    df = pd.read_csv(smart_open(path))
    i = 0
    while True and df.shape[0] >= i:
        yield (df.iloc[i:i + ATHENA_RESULTS_BATCH_SIZE, :])
        i = i + ATHENA_RESULTS_BATCH_SIZE
