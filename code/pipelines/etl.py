from prefect import flow, task
from prefect.cache_policies import NO_CACHE, INPUTS
from duckdb import Expression, ConstantExpression
from datetime import datetime, timedelta
from code.settings import DATA_PATH, QUERIES_PATH
from code.loggers import logger
from code.pipelines.state_handlers import completion_handler, failure_handler
from code.utils import get_currency_exchange_rate_as_polars, batch_operation
from code.connections import db_source, db_destination
import polars as pl



# Cache data retrieved for 7 days, based on input parameters
# NOTE: the fetched data must be idempotent to avoid data inconsistency!
@task(retries=2, retry_delay_seconds=5, on_failure=[failure_handler], cache_policy=INPUTS, cache_expiration=timedelta(days=7))
def extract_sales_data(date_start: datetime, date_end: datetime) -> pl.DataFrame:
    query_path = QUERIES_PATH.joinpath('car_sales.sql')
    with open(query_path, 'r') as f:
        query = f.read()
    
    sales_data_df = db_source.fetch_to_polars(query, parameters=[date_start, date_end])
    return sales_data_df
    

@task(retries=2, retry_delay_seconds=5, on_failure=[failure_handler], cache_policy=INPUTS, cache_expiration=timedelta(days=7))
def normalize_currency(sales_data: pl.DataFrame, target_currency_code: str = 'USD') -> pl.DataFrame:
    """Normalize sale price e.g. to USD"""
    # For testing purpose, get the API response from file
    exchange_list_df = get_currency_exchange_rate_as_polars(target_currency_code='USD')
    exchange_last_updated_at = exchange_list_df.select(pl.col('updated_at')).max().item()

    sales_data = sales_data.with_columns(pl.col('*'), pl.col('currency_name').alias('code'))

    sales_normalized = sales_data \
                        .join(
                            exchange_list_df, 
                            on='code',
                            how='inner') \
                        .select(
                            pl.col('*'), 
                            pl.col('sale_price').floordiv(pl.col('rate')).alias('sale_price_normalized'),
                            pl.lit(target_currency_code).alias('sale_price_normalized_currency'),
                            pl.lit(exchange_last_updated_at).alias('exchange_rate_date'))                     
    
    return sales_normalized


@task(retries=2, retry_delay_seconds=5, on_failure=[failure_handler], cache_policy=INPUTS, cache_expiration=timedelta(days=7))
def remove_invalid_data(sales_data: pl.DataFrame) -> pl.DataFrame:    
    # Simulate data validation
    sales_data_validated = sales_data \
                        .filter(pl.col('sale_price').__gt__(0)) \
                        .filter(pl.col('brand_id').is_not_null()) \
                        .filter(pl.col('car_id').is_not_null()) \
                        .filter(pl.col('currency_id').is_in([1,2,3,4])) \
                        .filter(pl.col('sale_price_normalized').__gt__(0)) \
                        .filter(pl.col('sale_price_normalized_currency').__ne__(pl.lit('')))
    
    return sales_data_validated


@task(retries=2, retry_delay_seconds=5, cache_policy=NO_CACHE, on_failure=[failure_handler])
def load_to_destination(sales_data: pl.DataFrame) -> None:
    with open(QUERIES_PATH / 'upsert_car_sales_dataset.sql', 'r' ) as f_ups: 
        upsert_query = f_ups.read()

    sales_data = sales_data \
                    .select('sale_id', 
                            'brand_id', 
                            'brand_name', 
                            'car_id',
                            'car_name',
                            'sale_price',
                            'currency_id',
                            'currency_name',
                            'sale_price_normalized',
                            'sale_price_normalized_currency',
                            'exchange_rate_date',
                            'transaction_date')
    
    # Batch insert per 1000 items
    batch_operation(db_destination, upsert_query, sales_data, 1000)


@task(retries=2, retry_delay_seconds=5, cache_policy=NO_CACHE, on_failure=[failure_handler])
def load_to_parquet(sales_data: pl.DataFrame, parquet_filename: str):
    if not parquet_filename.endswith('.parquet'):
        parquet_filename = parquet_filename + '.parquet'
    sales_data.write_parquet(DATA_PATH.joinpath('destinations', parquet_filename).__str__())


@flow(name='demo-car-sales', on_completion=[completion_handler], on_failure=[failure_handler])
def run():
    # Get data from last year
    last_year = datetime.now().year - 1
    date_start = datetime(year=last_year, month=1, day=1)
    date_end = datetime(year=last_year, month=12, day=31)

    sales_raw = extract_sales_data(date_start, date_end)
    sales_normalized = normalize_currency(sales_raw, target_currency_code='USD')
    sales_validated = remove_invalid_data(sales_normalized)

    min_date = date_start.strftime('%Y%m%d')
    max_date = date_end.strftime('%Y%m%d')
    load_to_destination(sales_validated)
    load_to_parquet(sales_validated, parquet_filename=f'car_sales_dataset__{min_date}-{max_date}.parquet')