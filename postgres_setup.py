import psycopg2
from psycopg2 import Error
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import pandas as pd
from typing import Optional, Dict, List, Union
import logging
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

""" Inicializa o conector com PostgreSQL """

    self.credentials = {
            'dbname': dbname,
            'user': user,
            'password': password,
            'host': host,
            'port': port
        }

self.setup_logging()

"""Configura o logging para registro de operações no diretório log_dir (criado)"""

 def setup_logging(self):
        log_dir = 'logs'
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)
            
        log_file = os.path.join(
            log_dir, 
            f'postgres_operations_{datetime.now().strftime("%Y%m%d")}.log'
        )
        
        logging.basicConfig(
            filename=log_file,
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )

"""Cria uma conexão com o banco de dados"""

def create_connection(self):
        try:
            conn = psycopg2.connect(**self.credentials)
            return conn
        except Error as e:
            logging.error(f"Erro ao conectar ao PostgreSQL: {str(e)}")
            raise

