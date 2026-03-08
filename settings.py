from dotenv import dotenv_values
import duckdb
import polars as pl
from pathlib import Path

ENV = dotenv_values('.env')
DB_SOURCE_HOST = ENV['DB_SOURCE_HOST']
DB_SOURCE_PORT = ENV['DB_SOURCE_PORT']
DB_SOURCE_NAME = ENV['DB_SOURCE_NAME']
DB_SOURCE_USER = ENV['DB_SOURCE_USER']
DB_SOURCE_PASSWORD  = ENV['DB_SOURCE_PASSWORD']
DB_SOURCE_TYPE = ENV['DB_SOURCE_TYPE']
WORK_POOL_NAME = ENV['WORK_POOL_NAME']
EXCHANGE_RATE_API_KEY = ENV['EXCHANGE_RATE_API_KEY']
LIMIT_LOG_WRITES_PER_HOUR = int(ENV['LIMIT_LOG_WRITES_PER_HOUR'])

BASE_PATH = Path(__file__).parent
DATA_PATH = BASE_PATH.joinpath('data')
QUERIES_PATH = BASE_PATH.joinpath('queries')

db_source_conn = duckdb.connect(DATA_PATH.joinpath('sources', 'db_source.db').__str__())
db_destination_conn = duckdb.connect(DATA_PATH.joinpath('destinations', 'db_destination.db').__str__())

# Initialize
duckdb.sql("ATTACH 'host={host} port={port} dbname={name} user={user} password={password}' AS {pgdb} (TYPE {dbtype});"\
    .format(host=DB_SOURCE_HOST,
            port=DB_SOURCE_PORT,
            name=DB_SOURCE_NAME,
            user=DB_SOURCE_USER,
            password=DB_SOURCE_PASSWORD, 
            pgdb=DB_SOURCE_NAME,
            dbtype=DB_SOURCE_TYPE))