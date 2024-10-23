import pytest
from unittest.mock import MagicMock, patch
from datetime import datetime
import pandas as pd
from generate_random_data import DbConfig, DataGenerator
from dotenv import load_dotenv
import os

# Carrega variáveis de ambiente
load_dotenv()

# Fixtures compartilhadas
@pytest.fixture
def db_config():
    return DbConfig(
        dbname="smart_data_db",
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD'),
        host="localhost",
        port='5432'
    )

@pytest.fixture
def data_generator(db_config):
    return DataGenerator(db_config)

@pytest.fixture
def mock_dataframes():
    df_origem = pd.DataFrame({
        'id_origem': [1],
        'nome_origem': ['Teste'],
        'tipo_dado': ['CSV'],
        'volume': [1000],
        'latencia': ['1h'],
        'descricao': ['Teste']
    })
    
    df_fluxo = pd.DataFrame({
        'id_fluxo': [1],
        'id_origem': [1],
        'destino': ['DW'],
        'status': ['Ativo'],
        'data_criacao': [datetime.now()],
        'data_atualizacao': [datetime.now()]
    })
    
    df_analises = pd.DataFrame({
        'id_analise': [1],
        'id_fluxo': [1],
        'hipoteses': ['Teste'],
        'resultado': ['OK'],
        'data_analise': [datetime.now()],
        'responsavel': ['Teste']
    })
    
    return df_origem, df_fluxo, df_analises

# Testes de inicialização
class TestInitialization:
    def test_db_config_initialization(self, db_config):
        assert isinstance(db_config.dbname, str)
        assert isinstance(db_config.user, str)
        assert isinstance(db_config.password, str)
        assert isinstance(db_config.host, str)
        assert isinstance(db_config.port, str)

    def test_data_generator_initialization(self, data_generator):
        assert hasattr(data_generator, 'gerar_dados_origem')
        assert hasattr(data_generator, 'gerar_fluxo_dados')
        assert hasattr(data_generator, 'gerar_analises')
        assert hasattr(data_generator, 'inserir_dados_no_banco')
        assert hasattr(data_generator, 'gerar_e_inserir_dados')

# Testes de geração de dados
class TestDataGeneration:
    def test_gerar_dados_origem(self, data_generator):
        num_registros = 100
        origem_df = data_generator.gerar_dados_origem(num_registros)
        
        assert isinstance(origem_df, pd.DataFrame)
        assert len(origem_df) == num_registros
        assert all(col in origem_df.columns for col in [
            'id_origem', 'nome_origem', 'tipo_dado', 
            'volume', 'latencia', 'descricao'
        ])

    def test_gerar_fluxo_dados(self, data_generator):
        num_registros = 200
        data_generator.gerar_dados_origem(100)  # Pré-requisito
        fluxo_df = data_generator.gerar_fluxo_dados(num_registros)
        
        assert isinstance(fluxo_df, pd.DataFrame)
        assert len(fluxo_df) == num_registros
        assert all(col in fluxo_df.columns for col in [
            'id_fluxo', 'id_origem', 'destino', 'status', 
            'data_criacao', 'data_atualizacao'
        ])
        
        # Validações específicas
        assert fluxo_df['id_origem'].isin(data_generator.df_origem['id_origem']).all()
        assert (fluxo_df['data_atualizacao'] > fluxo_df['data_criacao']).all()
        assert all(isinstance(dt, datetime) for dt in fluxo_df['data_criacao'])
        assert all(isinstance(dt, datetime) for dt in fluxo_df['data_atualizacao'])

    def test_gerar_analises(self, data_generator):
        num_registros = 300
        data_generator.gerar_dados_origem(100)  # Pré-requisitos
        data_generator.gerar_fluxo_dados(200)
        
        analise_df = data_generator.gerar_analises(num_registros)
        
        assert isinstance(analise_df, pd.DataFrame)
        assert len(analise_df) == num_registros
        assert all(col in analise_df.columns for col in [
            'id_analise', 'id_fluxo', 'hipoteses', 
            'resultado', 'data_analise', 'responsavel'
        ])
        assert analise_df['id_fluxo'].isin(data_generator.df_fluxo['id_fluxo']).all()

# Testes de inserção no banco
class TestDatabaseOperations:
    def test_inserir_dados_sem_dataframes(self, data_generator):
        with pytest.raises(ValueError) as exc_info:
            data_generator.inserir_dados_no_banco()
        assert "Gere todos os dados antes de inserir no banco" in str(exc_info.value)

    @patch('psycopg2.connect')
    def test_inserir_dados_sucesso(self, mock_connect, data_generator, mock_dataframes):
        df_origem, df_fluxo, df_analises = mock_dataframes
        data_generator.df_origem = df_origem
        data_generator.df_fluxo = df_fluxo
        data_generator.df_analises = df_analises

        mock_cursor = MagicMock()
        mock_connect.return_value.cursor.return_value.__enter__.return_value = mock_cursor
        
        data_generator.inserir_dados_no_banco()
        
        assert mock_cursor.executemany.call_count == 3
        mock_connect.return_value.commit.assert_called_once()

    @patch('psycopg2.connect')
    def test_inserir_dados_erro(self, mock_connect, data_generator, mock_dataframes):
        df_origem, df_fluxo, df_analises = mock_dataframes
        data_generator.df_origem = df_origem
        data_generator.df_fluxo = df_fluxo
        data_generator.df_analises = df_analises

        mock_cursor = MagicMock()
        mock_cursor.executemany.side_effect = Exception("Erro de teste")
        mock_connect.return_value.cursor.return_value.__enter__.return_value = mock_cursor
        
        with pytest.raises(Exception) as exc_info:
            data_generator.inserir_dados_no_banco()
        
        assert "Erro ao inserir dados" in str(exc_info.value)
        mock_connect.return_value.rollback.assert_called_once()

# Testes de geração e inserção integrados
class TestIntegration:
    def test_gerar_e_inserir_dados_padrao(self, data_generator):
        with patch.object(data_generator, 'inserir_dados_no_banco') as mock_insert:
            df_origem, df_fluxo, df_analises = data_generator.gerar_e_inserir_dados()
            
            assert isinstance(df_origem, pd.DataFrame)
            assert isinstance(df_fluxo, pd.DataFrame)
            assert isinstance(df_analises, pd.DataFrame)
            assert len(df_origem) == 100
            assert len(df_fluxo) == 200
            assert len(df_analises) == 300
            mock_insert.assert_called_once()

    def test_gerar_e_inserir_dados_customizado(self, data_generator):
        with patch.object(data_generator, 'inserir_dados_no_banco') as mock_insert:
            df_origem, df_fluxo, df_analises = data_generator.gerar_e_inserir_dados(
                num_origem=10,
                num_fluxo=20,
                num_analises=30
            )
            
            assert len(df_origem) == 10
            assert len(df_fluxo) == 20
            assert len(df_analises) == 30
            mock_insert.assert_called_once()

# Testes de gerenciamento de conexão
class TestConnectionManagement:
    def test_close_conexao_ativa(self, data_generator):
        mock_conn = MagicMock()
        data_generator.conn = mock_conn
        
        data_generator.close()
        
        mock_conn.close.assert_called_once()
        assert data_generator.conn is None

    def test_close_sem_conexao(self, data_generator):
        data_generator.conn = None
        data_generator.close()  # Não deve gerar erro
        assert data_generator.conn is None

    def test_context_manager(self, data_generator):
        with patch.object(data_generator, 'connect') as mock_connect:
            with patch.object(data_generator, 'close') as mock_close:
                with data_generator:
                    mock_connect.assert_called_once()
                mock_close.assert_called_once()