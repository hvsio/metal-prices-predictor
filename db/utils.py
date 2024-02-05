import logging
import os
from contextlib import contextmanager

from sqlalchemy import create_engine
from sqlalchemy.engine.base import Connection, Engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy_utils import create_database, database_exists


@contextmanager
def get_db_connection(force_create: False) -> (Engine, Connection):
    """First creation of job database based on .env values.
    As a context manager allow to execute statements with created engine and connection
    and ensures disposal of resources afterwards.

    Params:
        force_create (bool): whether to create the schema if it does not exists

    Returns:
        Engine: database engine
        Connection: database connection
    """

    db_url = (
        f"postgresql://{os.environ.get('db_user')}"
        f":{os.environ.get('db_pass')}"
        f"@{os.environ.get('db_host')}"
        f":{str(os.environ.get('db_port'))}"
        f"/{os.environ.get('db_name')}"
    )

    if not database_exists(db_url):
        if force_create:
            create_database(db_url)
        else:
            raise PermissionError('Could not create non-existing database...')

    try:
        engine = create_engine(db_url, echo=False)
        conn = engine.connect()
        yield engine, conn
    except (SQLAlchemyError, ValueError) as e:
        logging.error(
            f'Encountered error when establishing db environment: {e}'
        )
    finally:
        conn.close()
        engine.dispose()
