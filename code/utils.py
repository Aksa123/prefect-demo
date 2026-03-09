from code.settings import BASE_PATH, DATA_PATH, db_source_conn, db_destination_conn, EXCHANGE_RATE_API_KEY
from code.loggers import logger
import requests
import json



def close_databases(commit: bool = False):
    if commit:
        db_source_conn.commit()
        db_destination_conn.commit()
    db_source_conn.close()
    db_destination_conn.close()


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