from pathlib import Path
from time import sleep
import sqlite3
from code.settings import DATA_PATH
import polars as pl
import duckdb


class DBConnection:
    def __init__(self, path: Path | str ):
        self.path = path
        self.conn = sqlite3.connect(self.path)

        self.execute = self.exc_wrapper(self.conn.execute)
        self.executemany = self.exc_wrapper(self.conn.executemany)

        retries = self.retry_wrapper(5, 2)
        self.execute = retries(self.execute)
        self.executemany = retries(self.executemany)

    def reconnect(self):
        self.conn.close()
        self.conn = sqlite3.connect(self.path)

    def close(self):
        self.conn.close()
        
    def exc_wrapper(self, func):
        def inner(sql: str, parameters=None):
            # Auto-rollback upon failure
            try:
                res = func(sql, parameters)
                self.conn.commit()
                return res
            except Exception as err:
                self.conn.rollback()
                raise err
        return inner
    
    def retry_wrapper(self, count: int = 5, delay: int = 2):
        def outer(func):
            def inner(sql: str, parameters: list = None):
                for i in range(1, count+1):
                    try:
                        res = func(sql, parameters)
                        return res
                    # Only retry for connection-specific issues e.g. OperationalError
                    except sqlite3.OperationalError as operr:
                        print(f'error occurred. reconnecting database and retrying transaction... ( {i} / {count} )')
                        sleep(delay)
                        self.reconnect()
                print('retry attempt limit reached >_>')
                raise operr
            return inner
        return outer
    
    def fetch_to_duckdb(self, duck_conn: duckdb.DuckDBPyConnection, sql: str, parameters: list = None) -> duckdb.DuckDBPyRelation:
        if parameters != None:
            opts = {'parameters': parameters}
        else:
            opts = None
        df = pl.read_database(query=sql, connection=self, execute_options=opts).to_arrow()
        return duck_conn.from_arrow(df)


db_source = DBConnection(DATA_PATH / 'sources' / 'db_source.db')
db_destination = DBConnection(DATA_PATH / 'destinations' / 'db_destination.db')
