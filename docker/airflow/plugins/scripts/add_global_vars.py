from airflow import settings
from airflow.models import Connection, Variable
from sqlalchemy.orm import sessionmaker
from dotenv import dotenv_values
from sqlalchemy.exc import SQLAlchemyError, IntegrityError
from typing import Union

def add_airflow_config(session, obj: Union[Connection, Variable]):
    try:
        session.add(obj)
    except SQLAlchemyError as err:
        session.rollback()
        if not isinstance(err, IntegrityError):
            raise err

if __name__ == "__main__":
    env_vars = dotenv_values("./.env")

    Session = sessionmaker(bind=settings.engine)
    session = Session()

    for key, value in env_vars.items():
        variable = Variable(key=key, val=value)
        add_airflow_config(session, variable)
        
    session.commit()

    conn_aws = Connection(
        conn_id='aws',
        conn_type='aws',
        login=Variable.get('aws_access_key_id'),
        password=Variable.get('aws_secret_access_key'),
        extra={"region_name": Variable.get('aws_region')},
    )
    add_airflow_config(session, conn_aws)

    conn_metals_api = Connection(
        conn_id='metals_api',
        conn_type='http',
        host=Variable.get('api_url'),
        extra={"region_name": Variable.get('aws_region')},
    )
    add_airflow_config(session, conn_metals_api)

    conn_postgres = Connection(
        conn_id="postgres",
        conn_type="postgres",
        login=Variable.get('db_user'),
        password=Variable.get('db_pass'),
        port=Variable.get('db_port'),
        host='host.docker.internal',
    )
    add_airflow_config(session, conn_postgres)

    session.commit()
    session.close()
