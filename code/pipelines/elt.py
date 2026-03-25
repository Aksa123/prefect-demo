from prefect import flow, task
from prefect.cache_policies import NO_CACHE, INPUTS
from psycopg.sql import SQL, Identifier
from datetime import datetime, timedelta
from code.settings import BASE_PATH
from code.loggers import logger
from code.pipelines.state_handlers import completion_handler, failure_handler
from code.utils import get_currency_exchange_rate_as_polars, generate_upsert_query, batch_operation
from code.connections import db_source, db_destination
import polars as pl



# Cache data retrieved for 7 days, based on input parameters
# NOTE: the fetched data must be idempotent to avoid data inconsistency!
@task(retries=2, retry_delay_seconds=5, on_failure=[failure_handler], cache_policy=INPUTS, cache_expiration=timedelta(days=7))
def extract_table_data_by_dates(table_name: str, date_start: datetime, date_end: datetime) -> pl.DataFrame:
    query = SQL("select * from {} where coalesce(updated_at, created_at) between ? and ?").format(Identifier(table_name)).as_string()
    data = db_source.fetch_to_polars(query, parameters=[date_start, date_end])
    return data


@task(retries=2, retry_delay_seconds=5, on_failure=[failure_handler], cache_policy=NO_CACHE)
def load_table(table_name: str, pk_column_names: list[str], data: pl.DataFrame) -> None:
    if data.__len__() == 0:
        logger.debug('empty')
        return
    
    upsert_query = generate_upsert_query(table_name, pk_column_names, data.columns)

    # Batch insert per 1000 items
    batch_operation(db_destination, upsert_query, data, 1000)


# In ELT model, we can just perform transformations in the datawarehouse/destination side
@task(retries=2, retry_delay_seconds=5, on_failure=[failure_handler], cache_policy=NO_CACHE)
def transform_dimension_car_detail(date_start: datetime, date_end: datetime):
    with open(BASE_PATH / 'code' / 'queries' / 'dimension_car_detail.sql', 'r' ) as f:
        query = f.read()
    data = db_destination.fetch_to_polars(query, parameters=[date_start, date_end, date_start, date_end, date_start, date_end])
    load_table('dimension_car_detail', ['id'], data)


@task(retries=2, retry_delay_seconds=5, on_failure=[failure_handler], cache_policy=NO_CACHE)
def transform_fact_car_sales_dataset(date_start: datetime, date_end: datetime):
    with open(BASE_PATH / 'code' / 'queries' / 'fact_car_sales_dataset.sql', 'r' ) as f:
        query = f.read()
    data = db_destination.fetch_to_polars(query, parameters=[date_start, date_end])
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
    
    exchange_list = get_currency_exchange_rate_as_polars('USD')
    load_table('currency_rates', ['code'], exchange_list)

    transform_dimension_car_detail(date_start, date_end)
    transform_fact_car_sales_dataset(date_start, date_end)
