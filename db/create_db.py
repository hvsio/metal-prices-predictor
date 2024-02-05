import logging
import os

from dotenv import find_dotenv, load_dotenv
from sqlalchemy import (Boolean, Column, DateTime, Float, ForeignKeyConstraint,
                        Integer, MetaData, Sequence, String, Table, inspect,
                        text)
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.schema import CreateSchema
from utils import get_db_connection

logger = logging.getLogger('postgres_logger')

load_dotenv(find_dotenv())

schema_name = os.environ.get('schema_name')
tablename_metals = os.environ.get('tablename_metals')
tablename_metals_prices = os.environ.get('tablename_metals_prices')
if not (schema_name or tablename_metals_prices or tablename_metals):
    raise ValueError('Missing DB values.')

metadata_obj = MetaData(schema=schema_name)

metals = Table(
    tablename_metals,
    metadata_obj,
    Column('ticker', String(10), nullable=False, primary_key=True),
    Column('abbreviation', String(10), nullable=False, primary_key=False),
    Column('fullname', String(20), nullable=False),
    Column('active', Boolean, nullable=False, default=False),
)

metal_prices_table = Table(
    tablename_metals_prices,
    metadata_obj,
    Column(
        'id',
        Integer,
        Sequence('metals_sequence', start=1, increment=1),
        primary_key=True,
        autoincrement=True,
    ),
    Column('metal_type', String(20), nullable=True),
    ForeignKeyConstraint(
        ['metal_type'],
        ['metals.ticker'],
        name='fk_metal_id',
        onupdate='CASCADE',
        ondelete='SET NULL',
    ),
    Column('price', Float, nullable=False),
    Column('timestamp', DateTime, nullable=False),
)

with get_db_connection(True) as (engine, conn):
    try:
        logger.info(f'Established connection with db engine: {engine}')

        # create schema
        if not inspect(conn).has_schema(schema_name):
            conn.execute(CreateSchema(schema_name))
            conn.commit()

        # create tables
        metadata_obj.create_all(engine)

        # add autoincrement to ids
        conn.execute(
            text(
                """
                    ALTER TABLE metals_analytics.metal_prices ALTER COLUMN id SET DEFAULT nextval('metals_sequence');
                    """
            )
        )

        # create view with last 12h data for model training
        with open('../sql/12h_metal_prices_view.sql', 'r') as f:
            query = f.read()
            conn.execute(text(query))

        # seed the db with metals of initial interest
        with open('../sql/seed.sql', 'r') as f:
            query = f.read()
            conn.execute(text(query))

        conn.commit()
    except SQLAlchemyError as e:
        logger.error(f'Error establishing the DB: {e}')
        conn.rollback()
