import unittest
from code.settings import BASE_PATH
import polars as pl
from code.connections import db_source
from code.settings import duckdb_conn



get_path_name = lambda name: BASE_PATH.joinpath('tests', name).__str__()

class TestQueries(unittest.TestCase):

    def test_uwu(self):
        self.assertTrue(True)

    def test_fetch_duck(self):
        ddf = db_source.fetch_to_duckdb(sql='select * from brands where id = ?', parameters=[3])
        self.assertEqual(ddf.select('id', 'name').fetchone(), (3, 'Tesla') )
        
    def test_execute(self):
        res = db_source.execute(sql='select 10, True').fetchone()
        self.assertEqual(res, (10, True))

    def test_reconnect(self):
        res1 = db_source.execute(sql='select 10, True')
        self.assertIsNotNone(res1)
        db_source.close()
        db_source.reconnect()
        res2 = db_source.execute(sql='select 10, True')
        self.assertIsNotNone(res2)