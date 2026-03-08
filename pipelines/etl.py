from prefect import flow, task
from prefect.logging import get_run_logger
from prefect.cache_policies import TASK_SOURCE, INPUTS, NO_CACHE
from prefect.docker import DockerImage
from datetime import datetime, timedelta, UTC
import duckdb
from duckdb import Expression, ConstantExpression
from settings import BASE_PATH, DATA_PATH, QUERIES_PATH, EXCHANGE_RATE_API_KEY, db_source_conn, db_destination_conn
import requests
import json
import polars as pl
from loggers import logger
from pipelines.state_handlers import completion_handler, failure_handler



# No cache because DuckDBPyRelation is always bound to a DB connection
# If need to cache then must fetch first e.g. to DataFrame
@task(retries=2, retry_delay_seconds=5, cache_policy=NO_CACHE, on_failure=[failure_handler])
def extract_sales_data(date_start: datetime, date_end: datetime) -> duckdb.DuckDBPyRelation:
    query_path = QUERIES_PATH.joinpath('car_sales.sql')
    with open(query_path, 'r') as f:
        query = f.read()
    
    sales_data_df = db_source_conn \
                    .sql(query, params=[date_start, date_end])
        
    return sales_data_df
    

@task(retries=2, retry_delay_seconds=5, cache_policy=NO_CACHE, on_failure=[failure_handler])
def normalize_currency(sales_data: duckdb.DuckDBPyRelation, target_currency_code:str='USD') -> duckdb.DuckDBPyRelation:
    exchange_list_url = f'https://v6.exchangerate-api.com/v6/{EXCHANGE_RATE_API_KEY}/latest/{target_currency_code}'
    try:
        res = requests.get(exchange_list_url)
    except requests.exceptions.HTTPError as err:
        raise requests.exceptions.HTTPError(f'Error during exchange rate fetching: {err}')
    except Exception as err:
        raise err

    if 400 <= res.status_code <=499:
        raise ValueError(f'Invalid target currency: {target_currency_code}')
    
    exchange_list:dict = json.loads(res.content.decode())
    last_updated_at = datetime.fromtimestamp(exchange_list['time_last_update_unix'], tz=UTC)

    rows = [[code, rate] for code, rate in exchange_list['conversion_rates'].items()]
    schema = ['code', 'rate']
    exchange_list_df = pl.DataFrame(rows, schema=schema, orient='row')
    # Make sure datasets are in the same connection (db_source_conn) so they can be joined
    exchange_list_df = db_source_conn.sql('select * from exchange_list_df')

    sales_normalized = sales_data.set_alias('sales') \
                        .join(
                            exchange_list_df.set_alias('exchange'), 
                            condition=Expression('sales.currency_name').__eq__(Expression('exchange.code')),
                            how='inner') \
                        .select(
                            Expression('*'), 
                            Expression('sales.sale_price').__div__(Expression('exchange.rate')).alias('sale_price_normalized'),
                            ConstantExpression(target_currency_code).alias('sale_price_normalized_currency'),
                            ConstantExpression(last_updated_at).alias('exchange_rate_date'))                     
    
    return sales_normalized


@task(cache_policy=NO_CACHE, on_failure=[failure_handler])
def remove_invalid_data(sales_data: duckdb.DuckDBPyRelation) -> duckdb.DuckDBPyRelation:    
    # Simulate data validation
    sales_data_validated = sales_data \
                        .filter(Expression('sale_price').__gt__(0)) \
                        .filter(Expression('brand_id').isnotnull()) \
                        .filter(Expression('car_id').isnotnull()) \
                        .filter(Expression('currency_id').isin(1,2,3,4)) \
                        .filter(Expression('sale_price_normalized').__gt__(0)) \
                        .filter(Expression('sale_price_normalized_currency').__ne__(ConstantExpression('')))
    
    return sales_data_validated


@task(retries=2, retry_delay_seconds=5, cache_policy=NO_CACHE, on_failure=[failure_handler])
def load(sales_data: duckdb.DuckDBPyRelation, parquet_filename: str = None) -> None:
    with open(QUERIES_PATH.joinpath('upsert_car_sales_dataset.sql'), 'r' ) as f:
        insert_query = f.read()
    with open(QUERIES_PATH.joinpath('delete_car_sales_dataset.sql'), 'r' ) as f:
        delete_query = f.read() 

    sales_data = sales_data.select('sale_id', 
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
    while True:
        data = sales_data.fetchmany(1000)
        if not data:
            break
        # Sqlite doesn't support INSERT ON CONFLICT DO UPDATE, so delete first then insert again to avoid duplication
        db_destination_conn.executemany(delete_query, [(i[0],) for i in data])
        db_destination_conn.executemany(insert_query, data)
        db_destination_conn.commit()
        print(f'added {len(data)} items')

    if parquet_filename:
        if not parquet_filename.endswith('.parquet'):
            parquet_filename = parquet_filename + '.parquet'
        sales_data.write_parquet(DATA_PATH.joinpath('destinations', parquet_filename).__str__())


@flow(name='demo-car-sales', on_completion=[completion_handler], on_failure=[failure_handler])
def run() -> list[str]:
    # Get data from last year
    last_year = datetime.now().year - 1
    date_start = datetime(year=last_year, month=1, day=1)
    date_end = datetime(year=last_year, month=12, day=31)

    sales_raw = extract_sales_data(date_start, date_end)
    sales_normalized = normalize_currency(sales_raw, target_currency_code='USD')
    sales_validated = remove_invalid_data(sales_normalized)

    min_date = date_start.strftime('%Y%m%d')
    max_date = date_end.strftime('%Y%m%d')
    load(sales_validated, parquet_filename=f'car_sales_dataset__{min_date}-{max_date}.parquet')
    
    db_source_conn.commit()
    db_source_conn.close()
    db_destination_conn.commit()
    db_destination_conn.close()

    return True


