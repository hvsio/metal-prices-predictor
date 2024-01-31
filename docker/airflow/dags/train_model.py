from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
import logging
import datetime as dt
from dotenv import load_dotenv, find_dotenv
from airflow.models import Variable
from ml.model import Model
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

load_dotenv(find_dotenv())
# change it with native airflow logger
logger = logging.getLogger('metal_ingestion')

default_args = {
    'owner': Variable.get('metals_api_token'),
    'retry': 3,
    'retry_delay': dt.timedelta(minutes=5),
}

@dag(
    description="Train model on last 12h data",
    start_date=dt.datetime(2024, 1, 25),
    default_args=default_args)
def training():
    @task
    def train_model():
        s3hook = S3Hook('aws')
        postgres = PostgresHook(postgres_conn_id='postgres')
        data = postgres.get_pandas_df('SELECT * FROM metals_analytics.metals_training_data')
        model = Model(["XAUUSD", "XAGUSD", "XPTUSD", "XPDUSD"], 0, 0)
        model.train(data=data)
        model.save("/tmp/models", s3hook, Variable.get("bucket_name_model_data"))

    train_model()

training = training()
