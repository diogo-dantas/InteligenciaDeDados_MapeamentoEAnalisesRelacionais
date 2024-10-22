import unittest
from unittest.mock import MagicMock, patch, mock_open
import pandas as pd
import psycopg2
from psycopg2 import Error
import os
from datetime import datetime
import logging
from typing import Dict

class TestPostgresConnector(unittest.TestCase):
    
    def setUp(self):
        """Configuração inicial para cada teste"""
        self.test_credentials = {
            'dbname': 'test_db',
            'user': 'test_user',
            'password': 'test_pass',
            'host': 'localhost',
            'port': '5432'
        }
        # Usar credenciais de ambiente para CI/CD
        if os.getenv('CI'):
            self.test_credentials.update({
                'host': os.getenv('POSTGRES_HOST', 'localhost'),
                'user': os.getenv('POSTGRES_USER', 'test_user'),
                'password': os.getenv('POSTGRES_PASSWORD', 'test_pass'),
                'dbname': os.getenv('POSTGRES_DB', 'test_db')
            })
        
        self.connector = PostgresConnector(**self.test_credentials)

    def test_init(self):
        """
        Testa a inicialização do PostgresConnector
        """
        def test_criacao_padrao():
            # Verifica se as credenciais foram armazenadas corretamente
            self.assertEqual(self.connector.credentials, self.test_credentials)
            
        def test_criacao_log_dir():
            # Verifica se o diretório de logs foi criado
            self.assertTrue(os.path.exists('logs'))
        
        def test_parametros_invalidos():
            # Testa criação com parâmetros inválidos
            with self.assertRaises(TypeError):
                PostgresConnector(dbname=None, user=None, password=None)
        
        test_criacao_padrao()
        test_criacao_log_dir()
        test_parametros_invalidos()

    def test_setup_logging(self):
        """
        Testa a configuração do logging
        """
        def test_configuracao_padrao():
            # Mock para o logger
            with patch('logging.basicConfig') as mock_logging:
                self.connector.setup_logging()
                mock_logging.assert_called_once()
                
                # Verifica se os parâmetros do logging estão corretos
                args = mock_logging.call_args[1]
                self.assertEqual(args['level'], logging.INFO)
                self.assertIn('postgres_operations_', args['filename'])
        
        def test_criacao_diretorio():
            # Remove diretório de logs se existir
            with patch('os.path.exists') as mock_exists:
                mock_exists.return_value = False
                with patch('os.makedirs') as mock_makedirs:
                    self.connector.setup_logging()
                    mock_makedirs.assert_called_once_with('logs')
        
        test_configuracao_padrao()
        test_criacao_diretorio()

    def test_create_connection(self):
        """
        Testa a criação de conexão com o banco
        """
        def test_conexao_sucesso():
            with patch('psycopg2.connect') as mock_connect:
                # Configura o mock
                mock_connect.return_value = MagicMock()
                
                # Testa a conexão
                conn = self.connector.create_connection()
                
                # Verifica se connect foi chamado com as credenciais corretas
                mock_connect.assert_called_once_with(**self.test_credentials)
                self.assertIsNotNone(conn)
        
        def test_conexao_erro():
            with patch('psycopg2.connect') as mock_connect:
                # Simula erro de conexão
                mock_connect.side_effect = Error("Connection error")
                
                # Verifica se a exceção é propagada
                with self.assertRaises(Error):
                    self.connector.create_connection()
        
        test_conexao_sucesso()
        test_conexao_erro()

    def test_execute_query(self):
        """
        Testa a execução de queries
        """
        def test_select_query():
            test_df = pd.DataFrame({'col1': [1, 2], 'col2': ['a', 'b']})
            
            with patch('pandas.read_sql_query') as mock_read_sql:
                mock_read_sql.return_value = test_df
                
                # Testa query de select
                result = self.connector.execute_query("SELECT * FROM test")
                
                # Verifica se o resultado é um DataFrame
                self.assertIsInstance(result, pd.DataFrame)
                pd.testing.assert_frame_equal(result, test_df)
        
        def test_insert_query():
            with patch('psycopg2.connect') as mock_connect:
                mock_cursor = MagicMock()
                mock_connect.return_value.__enter__.return_value.cursor.return_value = mock_cursor
                
                # Testa query de insert
                self.connector.execute_query(
                    "INSERT INTO test (col1) VALUES (%s)",
                    params=(1,),
                    return_data=False
                )
                
                # Verifica se execute foi chamado corretamente
                mock_cursor.execute.assert_called_once()
        
        def test_query_erro():
            with patch('psycopg2.connect') as mock_connect:
                mock_connect.side_effect = Error("Query error")
                
                # Verifica se a exceção é propagada
                with self.assertRaises(Error):
                    self.connector.execute_query("SELECT * FROM test")
        
        test_select_query()
        test_insert_query()
        test_query_erro()

    def test_create_database_tables(self):
        """
        Testa a criação das tabelas
        """
        def test_criacao_tabelas():
            with patch.object(self.connector, 'execute_query') as mock_execute:
                # Executa criação das tabelas
                self.connector.create_database_tables()
                
                # Verifica se execute_query foi chamado para cada tabela
                self.assertEqual(mock_execute.call_count, 3)
                
                # Verifica se as chamadas incluem as tabelas corretas
                calls = mock_execute.call_args_list
                tables = ['dados_origem', 'fluxo_dados', 'analises']
                for call, table in zip(calls, tables):
                    self.assertIn(table, call[0][0])
        
        def test_erro_criacao():
            with patch.object(self.connector, 'execute_query') as mock_execute:
                mock_execute.side_effect = Error("Creation error")
                
                # Verifica se a exceção é propagada
                with self.assertRaises(Error):
                    self.connector.create_database_tables()
        
        test_criacao_tabelas()
        test_erro_criacao()

    def test_insert_data(self):
        """
        Testa a inserção de dados
        """
        def test_insercao_simples():
            test_data = {'col1': 1, 'col2': 'test'}
            
            with patch.object(self.connector, 'execute_query') as mock_execute:
                self.connector.insert_data('test_table', test_data)
                
                # Verifica se a query foi construída corretamente
                call_args = mock_execute.call_args[0]
                self.assertIn('INSERT INTO test_table', call_args[0])
                self.assertEqual(call_args[1], tuple(test_data.values()))
        
        def test_insercao_erro():
            with patch.object(self.connector, 'execute_query') as mock_execute:
                mock_execute.side_effect = Error("Insert error")
                
                # Verifica se a exceção é propagada
                with self.assertRaises(Error):
                    self.connector.insert_data('test_table', {'col1': 1})
        
        test_insercao_simples()
        test_insercao_erro()

    def test_get_table_info(self):
        """
        Testa a obtenção de informações da tabela
        """
        def test_info_tabela():
            test_df = pd.DataFrame({
                'column_name': ['id', 'name'],
                'data_type': ['integer', 'varchar'],
                'character_maximum_length': [None, 255],
                'is_nullable': ['NO', 'YES']
            })
            
            with patch.object(self.connector, 'execute_query') as mock_execute:
                mock_execute.return_value = test_df
                
                result = self.connector.get_table_info('test_table')
                
                # Verifica se o resultado é correto
                pd.testing.assert_frame_equal(result, test_df)
                
                # Verifica se a query foi chamada corretamente
                mock_execute.assert_called_once()
                self.assertIn('information_schema.columns', 
                            mock_execute.call_args[0][0])
        
        def test_info_erro():
            with patch.object(self.connector, 'execute_query') as mock_execute:
                mock_execute.side_effect = Error("Info error")
                
                # Verifica se a exceção é propagada
                with self.assertRaises(Error):
                    self.connector.get_table_info('test_table')
        
        test_info_tabela()
        test_info_erro()

    def tearDown(self):
        """Limpeza após cada teste"""
        # Limpa logs gerados durante os testes
        if os.path.exists('logs'):
            for file in os.listdir('logs'):
                if file.startswith('postgres_operations_'):
                    os.remove(os.path.join('logs', file))
            os.rmdir('logs')

if __name__ == '__main__':
    unittest.main()