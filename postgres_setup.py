import psycopg2
import pandas as pd
from datetime import datetime
import os


class PostgresConnector:
    def __init__(
        self,
        dbname: str,
        user: str,
        password: str,
        host: str = 'localhost',
        port: str = '5432'
    ):

# Inicializa o conector com PostgreSQL

    self.credentials = {
            'dbname': dbname,
            'user': user,
            'password': password,
            'host': host,
            'port': port
        }


