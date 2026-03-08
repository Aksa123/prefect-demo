from prefect import flow, task
from prefect.logging import get_run_logger
from prefect.cache_policies import TASK_SOURCE, INPUTS, NO_CACHE
from prefect.futures import wait
from prefect.assets import materialize
from prefect.docker import DockerImage
from datetime import datetime, timedelta
from time import sleep, perf_counter
import duckdb
from duckdb import Expression
from settings import DB_SOURCE_NAME, db_source_conn, db_destination_conn
from pipelines import etl



if __name__ == "__main__":
    t1 = perf_counter()
    etl.run()
    t2 = perf_counter()
    print(t2-t1)

