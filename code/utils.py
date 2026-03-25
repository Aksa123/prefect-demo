from code.settings import BASE_PATH, DATA_PATH, EXCHANGE_RATE_API_KEY, duckdb_conn
from code.loggers import logger
from code.connections import db_source, db_destination, DBConnection
import requests
import json
from psycopg.sql import SQL, Identifier, Placeholder
from pydantic import BaseModel
from enum import Enum
import duckdb
from datetime import datetime, UTC
import polars as pl
from collections import deque


class PlaceholderSign(Enum):
    """Python DB API 2.0 standard: https://peps.python.org/pep-0249/#paramstyle"""
    FORMAT = '%s'
    QMARK = '?'


def close_databases(commit: bool = False):
    if commit:
        db_source.conn.commit()
        db_destination.conn.commit()
    db_source.close()
    db_destination.close()


def get_currency_exchange_rate(target_currency_code: str = 'USD'):
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
    return exchange_list


def get_currency_exchange_rate_from_file(target_currency_code: str = 'USD'):
    """Simulate GET HTTP request to https://www.exchangerate-api.com/ without API key"""
    filename = 'exchange_rate.json'
    with open(DATA_PATH / filename, 'r') as f:
        res = f.read()
    js = json.loads(res)
    return js


def get_currency_exchange_rate_as_polars(target_currency_code: str = 'USD') -> pl.DataFrame:
    if not EXCHANGE_RATE_API_KEY:
        res = get_currency_exchange_rate_from_file(target_currency_code)
    else:
        res = get_currency_exchange_rate(target_currency_code)
    
    last_updated_at = datetime.fromtimestamp(res['time_last_update_unix'], tz=UTC)
    schema = ['code', 'rate']
    rows = [[code, rate] for code, rate in res['conversion_rates'].items()]
    exchange_list_df = pl.DataFrame(rows, schema=schema, orient='row')
    exchange_list_df = exchange_list_df.with_columns(pl.col('*'), pl.lit(last_updated_at).alias('updated_at'))
    return exchange_list_df


def get_currency_exchange_rate_as_df(target_currency_code: str = 'USD') -> duckdb.DuckDBPyRelation:
    exchange_list_df = get_currency_exchange_rate_as_polars(target_currency_code)
    exchange_list_df = exchange_list_df.to_arrow()
    exchange_list_df = duckdb_conn.from_arrow(exchange_list_df)
    return exchange_list_df
    

def generate_upsert_query(table_name: str, pk_column_names: list[str], column_list: list[str], placeholder_sign: PlaceholderSign = PlaceholderSign.QMARK) -> str:
    query = SQL("insert into {} ({}) values ({}) on conflict ({}) do update set {}") \
                .format(
                    Identifier(table_name),
                    SQL(", ").join([Identifier(col) for col in column_list]),
                    SQL(", ").join([SQL(placeholder_sign.value) for i in column_list]),
                    SQL(", ").join([Identifier(pk) for pk in pk_column_names]),
                    SQL(", ").join([SQL(" = ").join([Identifier(col), SQL(".").join([SQL("excluded"), Identifier(col)])]) for col in column_list])
                )
    return query.as_string()


def generate_insert_query(table_name: str, column_list: list[str], placeholder_sign: PlaceholderSign = PlaceholderSign.QMARK) -> str:
    query = SQL("insert into {} ({}) values ({})") \
                .format(
                    Identifier(table_name),
                    SQL(", ").join([Identifier(col) for col in column_list]),
                    SQL(", ").join([SQL(placeholder_sign.value) for i in column_list])
                )
    return query.as_string()


def generate_delete_query(table_name: str, pk_column_names: list[str], placeholder_sign: PlaceholderSign = PlaceholderSign.QMARK) -> str:
    """Delete by primary keys / unique columns"""
    query = SQL("delete from {} where {}") \
                .format(
                    Identifier(table_name),
                    SQL(" and ").join([SQL(" = ").join([Identifier(pk), SQL(placeholder_sign.value) ]) for pk in pk_column_names])
                )
    return query.as_string()


def batch_operation(conn: DBConnection, query: str, data: pl.DataFrame, limit: int = 1000):
    # Deque is faster than list because of O(1) complexity for append func
    data_list = deque()
    for count, item in enumerate(data.iter_rows(), start=1):
        data_list.append(item)
        if count % limit == 0:
            conn.executemany(query, data_list)
            logger.debug(f'added {len(data_list)} items')
            data_list.clear()
    # Execute the remaining data
    conn.executemany(query, data_list)
    logger.debug(f'added {len(data_list)} items')
    data_list.clear()