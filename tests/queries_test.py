import unittest
from datetime import datetime, timedelta, UTC
from time import sleep, perf_counter
import duckdb
from duckdb import Expression, ConstantExpression
from settings import BASE_PATH, DATA_PATH, QUERIES_PATH, EXCHANGE_RATE_API_KEY, db_source_conn, db_destination_conn
import requests
import json
from prefect import get_run_logger
import polars as pl
from polars import Expr



get_path_name = lambda name: BASE_PATH.joinpath('tests', name).__str__()

class TestQueries(unittest.TestCase):

    def test_uwu(self):
        self.assertTrue(True)

    def test_car_sales_query(self) -> duckdb.DuckDBPyRelation:
        # Get data from last year
        last_year = datetime.now().year - 1
        date_start = datetime(year=last_year, month=1, day=1)
        date_end = datetime(year=last_year, month=12, day=31)

        query_path = QUERIES_PATH.joinpath('car_sales.sql')
        with open(query_path, 'r') as f:
            query = f.read()
        
        sales_data_df = db_source_conn \
                        .sql(query, params=[date_start, date_end])
        
        sales_data_df.write_parquet(get_path_name('sales_data_df.parquet'))
    

    def test_normalize_currency(self):
        sales_data = db_source_conn.read_parquet(BASE_PATH.joinpath('tests', 'sales_data_df.parquet').__str__())
        target_currency_code = 'USD'

        # Dummy exchange list
        with open(BASE_PATH.joinpath('tests', 'exchange_list.json'), 'r') as f:
            exchange_list:dict = json.loads(f.read())
            last_updated_at = datetime.fromtimestamp(exchange_list['time_last_update_unix'], tz=UTC)

        rows = [[code, rate] for code, rate in exchange_list['conversion_rates'].items()]
        schema = ['code', 'rate']
        exchange_list_df = pl.DataFrame(rows, schema=schema, orient='row')
        
        # Make sure datasets are in the same DB connection (db_source) so they can be joined
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
        
        sales_normalized.write_parquet(get_path_name('sales_normalized.parquet'))                  


    def test_remove_invalid_data(self):
        sales_normalized = duckdb.read_parquet(BASE_PATH.joinpath('tests', 'sales_normalized.parquet').__str__())
        sales_data_validated = sales_normalized \
                        .filter(Expression('sale_price').__gt__(0)) \
                        .filter(Expression('brand_id').isnotnull()) \
                        .filter(Expression('car_id').isnotnull()) \
                        .filter(Expression('currency_id').isin(1,2,3,4)) \
                        .filter(Expression('sale_price_normalized').__gt__(0)) \
                        .filter(Expression('sale_price_normalized_currency').__ne__(ConstantExpression('')))
                
        self.assertLessEqual(sales_data_validated.__len__(), sales_normalized.__len__())

        sales_data_validated.write_parquet(get_path_name('sales_validated.parquet'))


    def test_load(self):
        sales_data = duckdb.read_parquet(get_path_name('sales_validated.parquet'))
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
        min_date = sales_data.min('transaction_date').fetchone()[0].strftime('%Y%m%d')
        max_date = sales_data.max('transaction_date').fetchone()[0].strftime('%Y%m%d')
        
        # Batch insert per 1000 items
        # while True:
        #     data = sales_data.fetchmany(1000)
        #     if not data:
        #         break
        #     # Sqlite doesn't support INSERT ON CONFLICT DO UPDATE, so delete first then insert again
        #     db_destination_conn.executemany(delete_query, [(i[0],) for i in data])
        #     db_destination_conn.executemany(insert_query, data)
        #     db_destination_conn.commit()

        sales_data.write_parquet(get_path_name(f'car_sales_dataset__{min_date}-{max_date}.parquet'))


        
if __name__ == '__main__':
    unittest.main()
