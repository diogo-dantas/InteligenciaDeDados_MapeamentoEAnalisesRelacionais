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

    """ Executa uma query e retorna os dados como DataFrame """

    def execute_query(
        self, 
        query: str, 
        params: tuple = None,
        return_data: bool = True
        ) -> Optional[pd.DataFrame]:
        try:
            with self.create_connection() as conn:
                if return_data:
                    if params:
                        return pd.read_sql_query(query, conn, params=params)
                    return pd.read_sql_query(query, conn)
                else:
                    cur = conn.cursor()
                    if params:
                        cur.execute(query, params)
                    else:
                        cur.execute(query)
                    conn.commit()
                    logging.info(f"Query executed successfully: {query[:100]}...")
                    return None
        except Error as e:
            logging.error(f"Erro ao executar a Query: {str(e)}\nQuery: {query}")
            raise


    """Cria as tabelas necessárias do banco de dados para o projeto """

    def create_database_tables(self):
        create_tables_sql = {
            'dados_origem': """
            CREATE TABLE IF NOT EXISTS dados_origem (
                id_origem SERIAL PRIMARY KEY,
                nome_origem VARCHAR(255) NOT NULL,
                tipo_dado VARCHAR(100) NOT NULL,
                volume INTEGER,
                latencia VARCHAR(50),
                descricao TEXT
            );
            """,
            
            'fluxo_dados': """
            CREATE TABLE IF NOT EXISTS fluxo_dados (
                id_fluxo SERIAL PRIMARY KEY,
                id_origem INTEGER NOT NULL,
                destino VARCHAR(255) NOT NULL,
                status VARCHAR(50) DEFAULT 'ativo',
                data_criacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                data_atualizacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (id_origem) REFERENCES dados_origem(id_origem)
            );
            """,
            
            'analises': """
            CREATE TABLE IF NOT EXISTS analises (
                id_analise SERIAL PRIMARY KEY,
                id_fluxo INTEGER NOT NULL,
                hipoteses TEXT,
                resultado TEXT,
                data_analise TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                responsavel VARCHAR(255),
                FOREIGN KEY (id_fluxo) REFERENCES fluxo_dados(id_fluxo)
            );
            """
        }

        for table_name, sql in create_tables_sql.items():
            logging.info(f"Creating table: {table_name}")
            self.execute_query(sql, return_data=False)

    """ Insere dados em uma tabela """            
    
    def insert_data(self, table: str, data: Dict):
        columns = ', '.join(data.keys())
        values = ', '.join(['%s'] * len(data))
        query = f"""
        INSERT INTO {table} ({columns})
        VALUES ({values})
        RETURNING *;
        """
        return self.execute_query(query, tuple(data.values()))       

    """ Retorna informações sobre a estrutura de uma tabela  aproveitando o método execute_query"""

    def get_table_info(self, table: str) -> pd.DataFrame:
        query = """
        SELECT 
            column_name, 
            data_type, 
            character_maximum_length,
            is_nullable
        FROM information_schema.columns
        WHERE table_name = %s;
        """
        return self.execute_query(query, (table,))         