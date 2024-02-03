from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
import datetime as dt
from airflow.models import Variable
from ml.model import Model
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from utils.error_decorator import error_check


default_args = {
    'owner': Variable.get('_airflow_owner'),
    'retry': 3,
    'retry_delay': dt.timedelta(minutes=5),
}

@dag(
    description="Train model on last 12h data",
    start_date=dt.datetime(2024, 1, 25),
    is_paused_upon_creation=False,
    default_args=default_args)
def training():

    @task
    @error_check
    def train_model():
        s3hook = S3Hook('aws')
        postgres = PostgresHook(postgres_conn_id='postgres')
        data = postgres.get_pandas_df(f'SELECT * FROM {Variable.get("schema_name")}.{Variable.get("view_training_data")}')
        model = Model(data['metal_type'].unique(), 0, 0)
        model.train(data=data)
        model.save("/tmp/models", s3hook, Variable.get("bucket_name_model_data"))

    train_model()

training()
