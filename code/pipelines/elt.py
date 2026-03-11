from prefect import flow, task
from prefect.logging import get_run_logger
from prefect.cache_policies import TASK_SOURCE, INPUTS, NO_CACHE
from prefect.futures import wait
from prefect.docker import DockerImage
import duckdb
from duckdb import Expression, ConstantExpression
import polars as pl
from datetime import datetime, timedelta, UTC
from code.settings import BASE_PATH, DATA_PATH, QUERIES_PATH, EXCHANGE_RATE_API_KEY, db_source_conn, db_destination_conn
from code.loggers import logger
from code.pipelines.state_handlers import completion_handler, failure_handler
from code.utils import get_currency_exchange_rate_as_df, generate_upsert_query, generate_insert_query, generate_delete_query
from psycopg.sql import SQL, Identifier



# No cache because DuckDBPyRelation is always bound to a DB connection
# If need to cache then must fetch first e.g. to DataFrame
@task(retries=2, retry_delay_seconds=5, cache_policy=NO_CACHE, on_failure=[failure_handler])
def extract_table_data_by_dates(table_name: str, date_start: datetime, date_end: datetime) -> duckdb.DuckDBPyRelation:
    query = SQL("select * from {} where coalesce(updated_at, created_at) between ? and ?").format(Identifier(table_name)).as_string()
    data = db_source_conn.sql(query, params=[date_start, date_end])
    return data


@task(retries=2, retry_delay_seconds=5, cache_policy=NO_CACHE, on_failure=[failure_handler])
def load_table(table_name: str, pk_column_names: list[str], data: duckdb.DuckDBPyRelation) -> None:
    if data.__len__() == 0:
        print('empty')
        return
    
    # In production e.g. with Postgres db we can simply use UPSERT query
    # Somehow SQLite doesn't work with UPSERT here
    insert_query = generate_insert_query(table_name, data.columns)
    delete_query = generate_delete_query(table_name, pk_column_names)
    # Find out index that contains pk/unique
    pk_index = []
    for n, p in enumerate(data.columns):
        if p in pk_column_names:
            pk_index.append(n)
    
    # Batch insert per 1000 items
    while True:
        items = data.fetchmany(1000)
        if not items:
            break
        # Sqlite doesn't support INSERT ON CONFLICT DO UPDATE, so delete first then insert again to avoid duplication
        db_destination_conn.executemany(delete_query, [ [i[k] for k in pk_index] for i in items ])
        db_destination_conn.executemany(insert_query, items)
        db_destination_conn.commit()
        print(f'added {len(items)} items')


# In ELT model, we can just perform transformations in the datawarehouse/destination side
@task(retries=2, retry_delay_seconds=5, cache_policy=NO_CACHE, on_failure=[failure_handler])
def transform_dimension_car_detail(date_start: datetime, date_end: datetime):
    with open(BASE_PATH / 'code' / 'queries' / 'dimension_car_detail.sql', 'r' ) as f:
        query = f.read()
    data = db_destination_conn.sql(query, params=[date_start, date_end, date_start, date_end, date_start, date_end])
    load_table('dimension_car_detail', ['id'], data)


@task(retries=2, retry_delay_seconds=5, cache_policy=NO_CACHE, on_failure=[failure_handler])
def transform_fact_car_sales_dataset(date_start: datetime, date_end: datetime):
    with open(BASE_PATH / 'code' / 'queries' / 'fact_car_sales_dataset.sql', 'r' ) as f:
        query = f.read()
    data = db_destination_conn.sql(query, params=[date_start, date_end])
    load_table('fact_car_sales_dataset', ['sale_id'], data)


@flow(name='demo-car-sales', on_completion=[completion_handler], on_failure=[failure_handler])
def run():
    # Get data from last year
    last_year = datetime.now().year - 1
    date_start = datetime(year=last_year, month=1, day=1)
    date_end = datetime(year=last_year, month=12, day=31)

    # Fetch and load data
    data_brands = extract_table_data_by_dates('brands', date_start, date_end)
    load_table('brands', ['id'], data_brands)

    data_types = extract_table_data_by_dates('types', date_start, date_end)
    load_table('types', ['id'], data_types)

    data_currencies = extract_table_data_by_dates('currencies', date_start, date_end)
    load_table('currencies', ['id'], data_currencies)

    data_cities = extract_table_data_by_dates('cities', date_start, date_end)
    load_table('cities', ['id'], data_cities)

    data_cars = extract_table_data_by_dates('cars', date_start, date_end)
    load_table('cars', ['id'], data_cars)

    data_customers = extract_table_data_by_dates('customers', date_start, date_end)
    load_table('customers', ['id'], data_customers)

    data_car_sales = extract_table_data_by_dates('car_sales', date_start, date_end)
    load_table('car_sales', ['id'], data_car_sales)
    
    exchange_list = get_currency_exchange_rate_as_df('USD')
    load_table('currency_rates', ['code'], exchange_list)

    transform_dimension_car_detail(date_start, date_end)
    transform_fact_car_sales_dataset(date_start, date_end)
