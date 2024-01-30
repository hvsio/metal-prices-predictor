from airflow.decorators import dag, task
import datetime as dt
from dotenv import load_dotenv, find_dotenv
from airflow.models import Variable
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.s3 import S3CreateBucketOperator
from airflow.models import Variable


load_dotenv(find_dotenv())
# change it with native airflow logger

default_args = {
    'owner': 'x',
    'retry': 3,
    'retry_delay': dt.timedelta(minutes=5),
}


@dag(
    schedule_interval=dt.timedelta(hours=1),
    description="Initialize metal pipeline",
    start_date=dt.datetime(2024, 1, 30),
    is_paused_upon_creation=True,
    default_args=default_args)
def intialize_aws():
    @task
    def check_for_bucket():
        s3 = S3Hook('aws')
        is_bucket_present = s3.check_for_bucket('raw_api_data')
        return is_bucket_present

    @task.branch(task_id="branching")
    def branching(bucket_exists):
        if bucket_exists:
            return 'bucket_ready'
        else:
            return 'create_bucket'

    @task()
    def bucket_ready():
        print('Bucket is already created.')

    create_bucket = S3CreateBucketOperator(
        aws_conn_id="aws",
        task_id="create_bucket",
        bucket_name=Variable.get("bucket_name"),
    )

    trigger_workflow = TriggerDagRunOperator(
        task_id="get_searchable_metals",
        trigger_dag_id="get_metals_prices",
        trigger_rule="none_failed"
    )

    branching(check_for_bucket()) >> [
        create_bucket, bucket_ready()] >> trigger_workflow


intialize_aws()
