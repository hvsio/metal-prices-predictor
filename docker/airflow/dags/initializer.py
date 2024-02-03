from airflow.decorators import dag, task
import datetime as dt
from airflow.models import Variable
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.s3 import S3CreateBucketOperator
from airflow.models import Variable
from utils.error_decorator import error_check


default_args = {
    'owner': Variable.get('metals_api_token'),
    'retry': 3,
    'retry_delay': dt.timedelta(minutes=5),
}

@dag(
    schedule_interval=dt.timedelta(hours=1),
    description="Initialize metal pipeline",
    start_date=dt.datetime(2024, 1, 25),
    is_paused_upon_creation=False,
    default_args=default_args)
def intialize_aws():

    @task
    @error_check
    def check_for_bucket_model_data():
        s3 = S3Hook('aws')
        is_bucket_present = s3.check_for_bucket(Variable.get("bucket_name_model_data"))
        return is_bucket_present

    
    @task.branch(task_id="branching_model")
    def branching_model(bucket_exists):
        if bucket_exists:
            return 'bucket_ready'
        else:
            return 'create_bucket_model_data'
        
    create_bucket_model_data = S3CreateBucketOperator(
        aws_conn_id="aws",
        task_id="create_bucket_model_data",
        bucket_name=Variable.get("bucket_name_model_data"),
    )
        
    @task
    @error_check
    def check_for_bucket_api_data():
        s3 = S3Hook('aws')
        is_bucket_present = s3.check_for_bucket(Variable.get("bucket_name_api_data"))
        return is_bucket_present

    @task.branch(task_id="branching_api")
    def branching_api(bucket_exists):
        if bucket_exists:
            return 'bucket_ready'
        else:
            return 'create_bucket_api_data'

    @task
    @error_check
    def bucket_ready():
        print('Bucket is already created.')

    create_bucket_api_data = S3CreateBucketOperator(
        aws_conn_id="aws",
        task_id="create_bucket_api_data",
        bucket_name=Variable.get("bucket_name_api_data"),
    )

    trigger_workflow = TriggerDagRunOperator(
        task_id="get_searchable_metals",
        trigger_dag_id="get_metals_prices",
        trigger_rule="none_failed"
    )

    branching_api(check_for_bucket_api_data()) >> [
        create_bucket_api_data, bucket_ready()] >> trigger_workflow
    
    branching_model(check_for_bucket_model_data()) >> [
        create_bucket_model_data, bucket_ready()] >> trigger_workflow


intialize_aws()
