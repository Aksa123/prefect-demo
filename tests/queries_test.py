import unittest
from datetime import datetime, timedelta, UTC
from time import sleep, perf_counter
import duckdb
from duckdb import Expression, ConstantExpression
from code.settings import BASE_PATH, QUERIES_PATH, duckdb_conn
from code.utils import get_currency_exchange_rate_from_file
from code.connections import db_source, db_destination
import polars as pl



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
        
        sales_data_df = db_source.fetch_to_duckdb(query, parameters=[date_start, date_end])
        # sales_data_df.write_parquet(get_path_name('sales_data_df.parquet'))
        duckdb_conn.register('sales_data_df', sales_data_df)
    

    def test_normalize_currency(self):
        # sales_data = duckdb_conn.read_parquet(BASE_PATH.joinpath('tests', 'sales_data_df.parquet').__str__())
        sales_data = duckdb_conn.sql('select * from sales_data_df')
        target_currency_code = 'USD'

        # Dummy exchange list
        exchange_list:dict = get_currency_exchange_rate_from_file('USD')
        last_updated_at = datetime.fromtimestamp(exchange_list['time_last_update_unix'], tz=UTC)

        rows = [[code, rate] for code, rate in exchange_list['conversion_rates'].items()]
        schema = ['code', 'rate']
        exchange_list_df = pl.DataFrame(rows, schema=schema, orient='row')
        
        # Make sure datasets are in the same DB connection (db_source) so they can be joined
        exchange_list_df = duckdb_conn.sql('select * from exchange_list_df')

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
        
        # sales_normalized.write_parquet(get_path_name('sales_normalized.parquet'))
        duckdb_conn.register('sales_normalized', sales_normalized)              


    def test_remove_invalid_data(self):
        # sales_normalized = duckdb_conn.read_parquet(BASE_PATH.joinpath('tests', 'sales_normalized.parquet').__str__())
        sales_normalized = duckdb_conn.sql('select * from sales_normalized')
        sales_validated = sales_normalized \
                        .filter(Expression('sale_price').__gt__(0)) \
                        .filter(Expression('brand_id').isnotnull()) \
                        .filter(Expression('car_id').isnotnull()) \
                        .filter(Expression('currency_id').isin(1,2,3,4)) \
                        .filter(Expression('sale_price_normalized').__gt__(0)) \
                        .filter(Expression('sale_price_normalized_currency').__ne__(ConstantExpression('')))
                
        self.assertLessEqual(sales_validated.__len__(), sales_normalized.__len__())
        duckdb_conn.register('sales_validated', sales_validated)
        
        
if __name__ == '__main__':
    unittest.main()
