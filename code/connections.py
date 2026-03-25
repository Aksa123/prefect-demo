from pathlib import Path
from typing import Self
from time import sleep
import polars as pl
import duckdb
import sqlite3
from code.settings import DATA_PATH, duckdb_conn
from code.loggers import logger


class DBConnection:
    def __init__(self, path: Path | str ):
        self.path = path
        self.conn = sqlite3.connect(self.path)

    def reconnect(self):
        self.conn.close()
        self.conn = sqlite3.connect(self.path)

    def close(self):
        self.conn.close()
        
    def exc_wrapper(func):
        def inner(self: Self, sql: str, parameters=[]):
            # Auto-commit & rollback upon failure
            try:
                res = func(self, sql, parameters)
                self.conn.commit()
                return res
            except Exception as err:
                self.conn.rollback()
                raise err
        return inner
    
    def retry_wrapper(count: int = 5, delay: int = 2):
        def outer(func):
            def inner(self: Self, sql: str, parameters: list = []):
                last_err = None
                for i in range(1, count+1):
                    try:
                        res = func(self, sql, parameters)
                        return res
                    # Only retry for connection-specific issues e.g. OperationalError
                    except (sqlite3.OperationalError, sqlite3.InternalError) as operr:
                        logger.error(f'error occurred. reconnecting database and retrying transaction... ( {i} / {count} )')
                        sleep(delay)
                        try:
                            self.reconnect()
                            logger.info('reconnected!')
                        except sqlite3.OperationalError:
                            pass
                        last_err = operr
                logger.error('retry attempt limit reached >_>')
                raise last_err
            return inner
        return outer
    
    def fetch_to_polars(self, sql: str, parameters: list = None):
        if parameters != None:
            opts = {'parameters': parameters}
        else:
            opts = None
        polars_df = pl.read_database(query=sql, connection=self, execute_options=opts)
        return polars_df
    
    def fetch_to_arrow(self, sql: str, parameters: list = None):
        arrow_df = self.fetch_to_polars(sql, parameters).to_arrow()
        return arrow_df 

    # Currently Duckdb cannot directly fetch from Polars dataframe
    def fetch_to_duckdb(self, sql: str, parameters: list = None) -> duckdb.DuckDBPyRelation:
        df = self.fetch_to_arrow(sql, parameters)
        return duckdb_conn.from_arrow(df)

    @retry_wrapper(5,2)
    @exc_wrapper
    def execute(self, sql: str, parameters=[]):
        return self.conn.execute(sql, parameters)
    
    @retry_wrapper(5,2)
    @exc_wrapper
    def executemany(self, sql: str, parameters=[]):
        return self.conn.executemany(sql, parameters)
    
    

db_source = DBConnection(DATA_PATH / 'sources' / 'db_source.db')
db_destination = DBConnection(DATA_PATH / 'destinations' / 'db_destination.db')
