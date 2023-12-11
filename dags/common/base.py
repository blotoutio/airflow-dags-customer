import json
import os


from connectors import athena as Athena

AWS_ACCESS_KEY_ID = os.environ['AWS_ACCESS_KEY_ID']
AWS_SECRET_ACCESS_KEY = os.environ['AWS_SECRET_ACCESS_KEY']
AWS_DEFAULT_REGION = os.environ['AWS_DEFAULT_REGION']
QUERY_LOGS_BUCKET = os.environ.get("QUERY_LOGS_BUCKET")

params = {
    'AWS_REGION': AWS_DEFAULT_REGION,
    'bucket': QUERY_LOGS_BUCKET,
    'path': f"s3://{QUERY_LOGS_BUCKET}/reverse-el",
    'AWS_ACCESS_KEY': AWS_ACCESS_KEY_ID,
    'AWS_SECRET_KEY': AWS_SECRET_ACCESS_KEY
}

session = Athena.connect(params)

