import datetime as dt
import os
import time

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from utils.error_decorator import error_check

default_args = {
    'owner': Variable.get('_airflow_owner'),
    'retry': 3,
    'retry_delay': dt.timedelta(minutes=5),
}


@dag(
    schedule_interval=dt.timedelta(hours=6),
    description='Backup database and models related to metals ingestion',
    start_date=dt.datetime(2024, 1, 25),
    is_paused_upon_creation=False,
    default_args=default_args,
)
def backup():
    BACKUP_DIR_NAME = Variable.get('backup_dir')

    def __extract_timestamp(filename: str) -> float:
        return float(filename.split('_')[1].replace('.gzip', ''))

    def __filter_by_interval(objects: list, hours: int):
        interval = int(
            ((dt.datetime.now() - dt.timedelta(hours=hours)).timestamp())
        )
        return [
            obj_id
            for obj_id in objects
            if __extract_timestamp(obj_id) > interval
        ]

    dump_postgres = BashOperator(
        task_id='dump_postgres',
        bash_command=f"pg_dump -h host.docker.internal -p {Variable.get('db_port')} -U {Variable.get('db_user')} -d {Variable.get('db_pass')} > {BACKUP_DIR_NAME}/postgres/backup{str(time.time()).split('.')[0]}.sql",
    )

    @task
    @error_check
    def cleanup_backups(subpath: str):
        path = f'{BACKUP_DIR_NAME}/{subpath}'
        backups = sorted(
            os.listdir(path), key=lambda x: __extract_timestamp(x)
        )
        while len(backups) > 20:
            oldest_backup = os.path.join(path, backups[0])
            os.remove(oldest_backup)
            backups = sorted(
                os.listdir(path), key=lambda x: __extract_timestamp(x)
            )

    @task
    @error_check
    def get_modelnames_to_backup() -> list:
        s3hook = S3Hook('aws')
        models_ids = s3hook.list_keys(
            bucket_name=Variable.get('bucket_name_model_data')
        )
        models_ids = __filter_by_interval(models_ids, 6)
        return models_ids

    @task
    @error_check
    def pull_models(models_ids: list):
        s3hook = S3Hook('aws')
        for m_id in models_ids:
            s3hook.download_file(
                key=m_id,
                bucket_name=Variable.get('bucket_name_model_data'),
                local_path=f'{BACKUP_DIR_NAME}/models',
                preserve_file_name=True,
            )

    pull_models(get_modelnames_to_backup()) >> cleanup_backups('models')
    dump_postgres >> cleanup_backups('postgres')


backup()
