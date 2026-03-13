import unittest
from datetime import datetime, timedelta, UTC
from time import sleep, perf_counter
import duckdb
from duckdb import Expression, ConstantExpression
from code.settings import BASE_PATH, DATA_PATH, QUERIES_PATH, EXCHANGE_RATE_API_KEY
from code.utils import get_currency_exchange_rate_from_file
import requests
import json
import polars as pl
from polars import Expr
from code.connections import db_source
from code.settings import duckdb_conn



get_path_name = lambda name: BASE_PATH.joinpath('tests', name).__str__()

class TestQueries(unittest.TestCase):

    def test_uwu(self):
        self.assertTrue(True)

    def test_fetch_duck(self):
        ddf = db_source.fetch_to_duckdb(sql='select * from brands where id = ?', parameters=[3])
        self.assertEqual(ddf.select('id', 'name').fetchone(), (3, 'Tesla') )
        