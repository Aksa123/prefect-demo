from prefect.docker import DockerImage
from datetime import datetime, timedelta
from time import sleep, perf_counter
from code.pipelines import etl
from code.utils import close_databases
from code.loggers import logger


if __name__ == "__main__":
    t1 = perf_counter()
    res_etl = etl.run()
    close_databases()
    t2 = perf_counter()
    
    print(f'Flow {etl.__name__} completed in {t2-t1} secods')
