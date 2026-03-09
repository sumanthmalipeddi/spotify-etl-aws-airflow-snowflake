from airflow import DAG
from airflow.providers.amazon.aws.operators.lambda_function import LambdaInvokeFunctionOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from datetime import datetime, timedelta

default_args = {
    "owner" : "sumanth",
    "depends_on_past" : False,
    "start_date" : datetime(2026,3,9)
}

dag = DAG(
    dag_id = "spotify_trigger_external",
    default_args= default_args,
    description= "DAG to trigger lambda function and check s3 upload",
    schedule= timedelta(days=1),
    catchup= False
)

trigger_extract_lambda = LambdaInvokeFunctionOperator(
    task_id = 'trigger_extract_lambda',
    function_name='spotifydata_extract',
    aws_conn_id = 'aws_s3_spotify',
    region_name = 'us-east-1',
    dag = dag
)

check_s3_upload = S3KeySensor(
    task_id = "check_s3_upload",
    bucket_key="s3://spotify-etl-pipeline-sumanth-dec25/raw_data/to_processed/*",
    wildcard_match=True,
    aws_conn_id = 'aws_s3_spotify',
    timeout = 60 * 60, # wait for one hour
    poke_interval = 60,
    dag = dag
)

trigger_transform_lambda = LambdaInvokeFunctionOperator(
    task_id = 'trigger_transform_lambda',
    function_name='spotify_transformation_load_function',
    aws_conn_id = 'aws_s3_spotify',
    region_name = 'us-east-1',
    dag = dag
)



trigger_extract_lambda >> check_s3_upload >> trigger_transform_lambda