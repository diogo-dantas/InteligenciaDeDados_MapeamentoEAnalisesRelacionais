import unittest
from unittest.mock import MagicMock, patch
import pandas as pd
from psycopg2 import Error
import os
from datetime import datetime
import logging
from dotenv import load_dotenv
from postgres_setup import PostgresConnector  # Importação da classe principal

# Carrega as variáveis de ambiente
load_dotenv()

class BaseTestPostgresConnector(unittest.TestCase):
    """Base test class with common setup and teardown logic"""
    
    @classmethod
    def setUpClass(cls):
        """Set up test environment once for all tests"""
        # Carrega as credenciais do ambiente
        cls.test_credentials = {
            'dbname': 'smart_data_db',
            'user': os.getenv('DB_USER'),
            'password': os.getenv('DB_PASSWORD'),
            'host': 'localhost',
            'port': '5432'
        }
        
        # Verifica se as credenciais necessárias estão presentes
        required_credentials = ['user', 'password']
        missing_credentials = [cred for cred in required_credentials 
                             if not cls.test_credentials[cred]]
        if missing_credentials:
            raise ValueError(
                f"Missing required environment variables: {missing_credentials}"
            )
    
    def setUp(self):
        """Set up test environment for each test"""
        try:
            self.connector = PostgresConnector(**self.test_credentials)
            self.sample_dataframe = pd.DataFrame({
                'col1': [1, 2],
                'col2': ['a', 'b']
            })
        except Exception as e:
            self.fail(f"Failed to initialize PostgresConnector: {str(e)}")
    
    def setUp(self):
        """Set up test environment for each test"""
        try:
            self.connector = PostgresConnector(**self.test_credentials)
            self.sample_dataframe = pd.DataFrame({
                'col1': [1, 2],
                'col2': ['a', 'b']
            })
        except Exception as e:
            self.fail(f"Failed to initialize PostgresConnector: {str(e)}")
    
    def tearDown(self):
        """Clean up after each test"""
        if os.path.exists('logs'):
            for file in os.listdir('logs'):
                if file.startswith('postgres_operations_'):
                    os.remove(os.path.join('logs', file))
            os.rmdir('logs')
            

class TestPostgresConnectorInitialization(BaseTestPostgresConnector):
    """Test cases for initialization and logging setup"""
    
    def test_initialization_with_valid_credentials(self):
        """Test successful initialization with valid credentials"""
        self.assertEqual(self.connector.credentials, self.test_credentials)
        self.assertTrue(os.path.exists('logs'))
    
    def test_initialization_with_invalid_credentials(self):
        """Test initialization with invalid credentials"""
        with self.assertRaises(TypeError):
            PostgresConnector(dbname=None, user=None, password=None)
    
    def test_logging_setup(self):
        """Test logging configuration"""
        with patch('logging.basicConfig') as mock_logging:
            self.connector.setup_logging()
            
            # Verify logging configuration
            args = mock_logging.call_args[1]
            self.assertEqual(args['level'], logging.INFO)
            self.assertIn('postgres_operations_', args['filename'])
            self.assertIn('%(asctime)s - %(levelname)s - %(message)s', 
                         args['format'])
    
    def test_log_directory_creation(self):
        """Test log directory creation when it doesn't exist"""
        with patch('os.path.exists') as mock_exists:
            mock_exists.return_value = False
            with patch('os.makedirs') as mock_makedirs:
                self.connector.setup_logging()
                mock_makedirs.assert_called_once_with('logs')

class TestPostgresConnectorDatabase(BaseTestPostgresConnector):
    """Test cases for database operations"""
    
    def test_successful_connection(self):
        """Test successful database connection"""
        with patch('psycopg2.connect') as mock_connect:
            mock_connect.return_value = MagicMock()
            conn = self.connector.create_connection()
            
            mock_connect.assert_called_once_with(**self.test_credentials)
            self.assertIsNotNone(conn)
    
    def test_failed_connection(self):
        """Test database connection failure"""
        with patch('psycopg2.connect') as mock_connect:
            mock_connect.side_effect = Error("Connection error")
            with self.assertRaises(Error):
                self.connector.create_connection()

class TestPostgresConnectorQueries(BaseTestPostgresConnector):
    """Test cases for query execution"""
    
    def test_select_query_execution(self):
        """Test SELECT query execution"""
        with patch('pandas.read_sql_query') as mock_read_sql:
            mock_read_sql.return_value = self.sample_dataframe
            
            result = self.connector.execute_query("SELECT * FROM test")
            
            self.assertIsInstance(result, pd.DataFrame)
            pd.testing.assert_frame_equal(result, self.sample_dataframe)
    
    def test_insert_query_execution(self):
        """Test INSERT query execution"""
        with patch('psycopg2.connect') as mock_connect:
            mock_cursor = MagicMock()
            mock_connect.return_value.__enter__.return_value.cursor.return_value = mock_cursor
            
            self.connector.execute_query(
                "INSERT INTO test (col1) VALUES (%s)",
                params=(1,),
                return_data=False
            )
            
            mock_cursor.execute.assert_called_once()
    
    def test_query_execution_error(self):
        """Test query execution error handling"""
        with patch('psycopg2.connect') as mock_connect:
            mock_connect.side_effect = Error("Query error")
            with self.assertRaises(Error):
                self.connector.execute_query("SELECT * FROM test")

class TestPostgresConnectorTableOperations(BaseTestPostgresConnector):
    """Test cases for table operations"""
    
    def test_table_creation(self):
        """Test database tables creation"""
        with patch.object(self.connector, 'execute_query') as mock_execute:
            self.connector.create_database_tables()
            
            self.assertEqual(mock_execute.call_count, 3)
            calls = mock_execute.call_args_list
            expected_tables = ['dados_origem', 'fluxo_dados', 'analises']
            
            for call, table in zip(calls, expected_tables):
                self.assertIn(table, call[0][0])
    
    def test_table_creation_error(self):
        """Test table creation error handling"""
        with patch.object(self.connector, 'execute_query') as mock_execute:
            mock_execute.side_effect = Error("Creation error")
            with self.assertRaises(Error):
                self.connector.create_database_tables()

class TestPostgresConnectorDataOperations(BaseTestPostgresConnector):
    """Test cases for data operations"""
    
    def test_data_insertion(self):
        """Test data insertion"""
        test_data = {'col1': 1, 'col2': 'test'}
        
        with patch.object(self.connector, 'execute_query') as mock_execute:
            self.connector.insert_data('test_table', test_data)
            
            call_args = mock_execute.call_args[0]
            self.assertIn('INSERT INTO test_table', call_args[0])
            self.assertEqual(call_args[1], tuple(test_data.values()))
    
    def test_data_insertion_error(self):
        """Test data insertion error handling"""
        with patch.object(self.connector, 'execute_query') as mock_execute:
            mock_execute.side_effect = Error("Insert error")
            with self.assertRaises(Error):
                self.connector.insert_data('test_table', {'col1': 1})
    
    def test_table_info_retrieval(self):
        """Test table information retrieval"""
        expected_info = pd.DataFrame({
            'column_name': ['id', 'name'],
            'data_type': ['integer', 'varchar'],
            'character_maximum_length': [None, 255],
            'is_nullable': ['NO', 'YES']
        })
        
        with patch.object(self.connector, 'execute_query') as mock_execute:
            mock_execute.return_value = expected_info
            
            result = self.connector.get_table_info('test_table')
            
            pd.testing.assert_frame_equal(result, expected_info)
            self.assertIn('information_schema.columns', 
                         mock_execute.call_args[0][0])
    
    def test_table_info_retrieval_error(self):
        """Test table information retrieval error handling"""
        with patch.object(self.connector, 'execute_query') as mock_execute:
            mock_execute.side_effect = Error("Info error")
            with self.assertRaises(Error):
                self.connector.get_table_info('test_table')

if __name__ == '__main__':
    unittest.main()