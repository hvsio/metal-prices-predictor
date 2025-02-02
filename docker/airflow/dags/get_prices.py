import datetime as dt
import json
import logging

from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException
from airflow.models import Variable
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.s3 import S3DeleteObjectsOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from custom_hooks.parametrizedHttp import ParametizedHttpHook
from utils.error_decorator import error_check

logger = logging.getLogger('metal_ingestion')

default_args = {
    'owner': Variable.get('_airflow_owner'),
    'retry': 3,
    'retry_delay': dt.timedelta(minutes=5),
}


@dag(
    schedule_interval=None,
    description='Metal prices ingestion pipeline',
    start_date=dt.datetime(2024, 1, 25),
    is_paused_upon_creation=False,
    default_args=default_args,
)
def get_metals_prices():
    @task
    @error_check
    def get_searchable_metals():
        postgres = PostgresHook(postgres_conn_id='postgres')
        results = postgres.get_records(
            f'SELECT abbreviation FROM {Variable.get("schema_name")}.{Variable.get("tablename_metals")}'
        )
        results = [entry[0] for entry in results]
        return results

    @task
    @error_check
    def get_metal_data(results):
        hook = ParametizedHttpHook('metals_api')
        resp = hook.run(
            '/v1/latest',
            params={
                'api_key': Variable.get('metals_api_token'),
                'base': 'USD',
                'currencies': ','.join(results),
            },
        )
        prices = {k + 'USD': v for k, v in resp.json()['rates'].items()}
        timestamp = str(resp.json()['timestamp'])
        s3hook = S3Hook('aws')
        s3hook.load_string(
            string_data=json.dumps(prices),
            key=f'{timestamp}.json',
            bucket_name=Variable.get('bucket_name_api_data'),
            replace=True,
        )

        return timestamp

    @task
    @error_check
    def insert_metal_data(timestamp):

        s3hook = S3Hook('aws')
        postgres = PostgresHook(postgres_conn_id='postgres')
        data = s3hook.get_key(
            f'{timestamp}.json',
            bucket_name=Variable.get('bucket_name_api_data'),
        )
        if not data:
            raise AirflowFailException(
                'Could not retrieve data from S3 bucket'
            )
        prices = data.get()['Body'].read().decode('utf-8')
        prices = json.loads(prices)
        prices = [
            (k, v, dt.datetime.fromtimestamp(int(timestamp)))
            for k, v in prices.items()
        ]
        postgres.insert_rows(
            'metals_analytics.metal_prices',
            prices,
            ['metal_type', 'price', 'timestamp'],
        )
        return timestamp

    delete_ingested_obj = S3DeleteObjectsOperator(
        aws_conn_id='aws',
        task_id='delete_ingested_obj',
        bucket=Variable.get('bucket_name_api_data'),
        keys="{{ ti.xcom_pull(task_ids='insert_metal_data') }}.json",
    )

    trigger_training = TriggerDagRunOperator(
        task_id='trigger_training', trigger_dag_id='training'
    )

    (
        insert_metal_data(get_metal_data(get_searchable_metals()))
        >> delete_ingested_obj
        >> trigger_training
    )


get_metals_prices()
