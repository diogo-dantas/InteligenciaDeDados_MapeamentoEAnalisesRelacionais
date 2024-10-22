import pytest
from unittest.mock import MagicMock
from datetime import datetime
import pandas as pd
from generate_random_data import DbConfig, DataGenerator

def test_db_config_initialization():
    config = DbConfig()
    assert isinstance(config.host, str)
    assert isinstance(config.port, (int, str))
    assert isinstance(config.dbname, str)
    assert isinstance(config.user, str)
    assert isinstance(config.password, str)

def test_data_generator_initialization():
    generator = DataGenerator()
    assert hasattr(generator, 'gerar_dados_origem')
    assert hasattr(generator, 'gerar_fluxo_dados')
    assert hasattr(generator, 'gerar_analises')
    assert hasattr(generator, 'inserir_dados_no_banco')
    assert hasattr(generator, 'gerar_e_inserir_dados')

def test_gerar_dados_origem():
    generator = DataGenerator()
    num_registros = 100
    origem_df = generator.gerar_dados_origem(num_registros)
    
    assert isinstance(origem_df, pd.DataFrame)
    assert len(origem_df) == num_registros
    assert  'id_origem' in origem_df.columns
    assert 'nome_origem' in origem_df.columns
    assert 'tipo_dado' in origem_df.columns
    assert 'volume' in origem_df.columns
    assert 'latencia' in origem_df.columns
    assert 'descricao' in origem_df.columns
    
def test_gerar_fluxo_dados():
    generator = DataGenerator()
    num_registros = 200
    fluxo_df = generator.gerar_fluxo_dados(num_registros)
    
    assert isinstance(fluxo_df, pd.DataFrame)
    assert len(fluxo_df) == num_registros
    assert 'id_fluxo' in fluxo_df.columns
    assert 'id_origem' in fluxo_df.columns
    assert 'destino' in fluxo_df.columns
    assert 'status' in fluxo_df.columns
    assert 'data_criacao' in fluxo_df.columns
    assert 'data_atualizacao' in fluxo_df.columns
    
    assert fluxo_df['id_origem'].isin(id_origem).all()
    assert (fluxo_df['data_atualizacao'] > fluxo_df['data_criacao'] ).all()
    assert isinstance(fluxo_df['data_atualizacao'].iloc[0], datetime)
    assert isinstance(fluxo_df['data_criacao'].iloc[0], datetime)

def test_gerar_analises():
    generator = DataGenerator()
    num_registros = 300
    
    analise_df= generator.gerar_analises(num_registros)
    
    assert isinstance(analise_df, pd.DataFrame)
    assert len(analise_df) == num_registros
    assert 'id_analise' in analise_df.columns
    assert 'id_fluxo' in analise_df.columns
    assert 'hipoteses' in analise_df.columns
    assert 'resultado' in analise_df.columns
    assert 'data_analise' in analise_df.columns
    assert 'responsavel' in analise_df.columns
    
    # Verify data types and constraints
    assert analise_df['id_fluxo'].isin(id_fluxo).all()

def test_inserir_dados_no_banco(self):
    """
    Testa diferentes cenários para o método inserir_dados_no_banco()
    """
    # Cenário 1: Teste quando os DataFrames não foram gerados
    def test_dataframes_nao_gerados():
        self.df_origem = None
        self.df_fluxo = None
        self.df_analises = None
        
        with self.assertRaises(ValueError) as context:
            self.inserir_dados_no_banco()
        self.assertTrue("Gere todos os dados antes de inserir no banco" in str(context.exception))
    
    # Cenário 2: Teste de inserção bem sucedida
    def test_insercao_sucesso():
        # Prepara os dados de teste
        self.df_origem = pd.DataFrame({
            'id_origem': [1],
            'nome_origem': ['Teste'],
            'tipo_dado': ['CSV'],
            'volume': [1000],
            'latencia': ['1h'],
            'descricao': ['Teste']
        })
        
        self.df_fluxo = pd.DataFrame({
            'id_fluxo': [1],
            'id_origem': [1],
            'destino': ['DW'],
            'status': ['Ativo'],
            'data_criacao': [datetime.now()],
            'data_atualizacao': [datetime.now()]
        })
        
        self.df_analises = pd.DataFrame({
            'id_analise': [1],
            'id_fluxo': [1],
            'hipoteses': ['Teste'],
            'resultado': ['OK'],
            'data_analise': [datetime.now()],
            'responsavel': ['Teste']
        })
        
        # Mock da conexão e cursor
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        self.conn = mock_conn
        
        # Executa o método
        self.inserir_dados_no_banco()
        
        # Verifica se os métodos esperados foram chamados
        self.assertEqual(mock_cursor.executemany.call_count, 3)
        mock_conn.commit.assert_called_once()
    
    # Cenário 3: Teste de erro na inserção
    def test_erro_insercao():
        # Prepara os dados
        self.df_origem = pd.DataFrame({'id_origem': [1]})  # DataFrame mínimo para teste
        self.df_fluxo = pd.DataFrame({'id_fluxo': [1]})
        self.df_analises = pd.DataFrame({'id_analise': [1]})
        
        # Mock da conexão com erro
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.executemany.side_effect = Exception("Erro de teste")
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        self.conn = mock_conn
        
        # Verifica se a exceção é lançada
        with self.assertRaises(Exception) as context:
            self.inserir_dados_no_banco()
        
        # Verifica se o rollback foi chamado
        mock_conn.rollback.assert_called_once()
    
    # Cenário 4: Teste de reconexão quando conexão está fechada
    def test_reconexao():
        self.df_origem = pd.DataFrame({'id_origem': [1]})
        self.df_fluxo = pd.DataFrame({'id_fluxo': [1]})
        self.df_analises = pd.DataFrame({'id_analise': [1]})
        
        # Simula conexão fechada
        self.conn = None
        
        # Mock do método connect
        original_connect = self.connect
        connect_called = False
        
        def mock_connect():
            nonlocal connect_called
            connect_called = True
            self.conn = MagicMock()
            return self.conn
        
        self.connect = mock_connect
        
        try:
            self.inserir_dados_no_banco()
            self.assertTrue(connect_called)
        finally:
            # Restaura o método original
            self.connect = original_connect

    # Executa todos os testes
    test_dataframes_nao_gerados()
    test_insercao_sucesso()
    test_erro_insercao()
    test_reconexao()

def test_gerar_e_inserir_dados(self):
    """
    Testa diferentes cenários para o método gerar_e_inserir_dados()
    """
    def test_geracao_padrao():
        # Mock dos métodos de geração e inserção
        self.gerar_dados_origem = MagicMock()
        self.gerar_fluxo_dados = MagicMock()
        self.gerar_analises = MagicMock()
        self.inserir_dados_no_banco = MagicMock()
        
        # Cria DataFrames de teste
        self.df_origem = pd.DataFrame({'test': [1]})
        self.df_fluxo = pd.DataFrame({'test': [1]})
        self.df_analises = pd.DataFrame({'test': [1]})
        
        # Executa o método
        df_origem, df_fluxo, df_analises = self.gerar_e_inserir_dados()
        
        # Verifica se os métodos foram chamados com os valores padrão
        self.gerar_dados_origem.assert_called_once_with(100)
        self.gerar_fluxo_dados.assert_called_once_with(200)
        self.gerar_analises.assert_called_once_with(300)
        self.inserir_dados_no_banco.assert_called_once()
        
        # Verifica se os DataFrames foram retornados corretamente
        self.assertIs(df_origem, self.df_origem)
        self.assertIs(df_fluxo, self.df_fluxo)
        self.assertIs(df_analises, self.df_analises)
    
    def test_geracao_parametros_customizados():
        # Mock dos métodos
        self.gerar_dados_origem = MagicMock()
        self.gerar_fluxo_dados = MagicMock()
        self.gerar_analises = MagicMock()
        self.inserir_dados_no_banco = MagicMock()
        
        # Executa com parâmetros customizados
        self.gerar_e_inserir_dados(num_origem=10, num_fluxo=20, num_analises=30)
        
        # Verifica se os métodos foram chamados com os valores corretos
        self.gerar_dados_origem.assert_called_once_with(10)
        self.gerar_fluxo_dados.assert_called_once_with(20)
        self.gerar_analises.assert_called_once_with(30)
    
    def test_erro_na_geracao():
        # Mock com erro
        self.gerar_dados_origem = MagicMock(side_effect=Exception("Erro teste"))
        
        # Verifica se a exceção é propagada
        with self.assertRaises(Exception) as context:
            self.gerar_e_inserir_dados()
        
        # Verifica se a mensagem de erro está correta
        self.assertEqual(str(context.exception), "Erro teste")
        
        # Verifica se os outros métodos não foram chamados
        self.assertFalse(hasattr(self, 'df_origem'))
    
    # Executa todos os testes de geração
    test_geracao_padrao()
    test_geracao_parametros_customizados()
    test_erro_na_geracao()

def test_close(self):
    """
    Testa diferentes cenários para o método close()
    """
    def test_fechamento_conexao_ativa():
        # Cria mock da conexão
        mock_conn = MagicMock()
        self.conn = mock_conn
        
        # Executa o método
        self.close()
        
        # Verifica se o método close foi chamado
        mock_conn.close.assert_called_once()
        # Verifica se a conexão foi definida como None
        self.assertIsNone(self.conn)
    
    def test_fechamento_sem_conexao():
        # Define conexão como None
        self.conn = None
        
        # Executa o método (não deve gerar erro)
        self.close()
        
        # Verifica se a conexão continua None
        self.assertIsNone(self.conn)
    
    def test_erro_ao_fechar():
        # Cria mock da conexão que gera erro ao fechar
        mock_conn = MagicMock()
        mock_conn.close.side_effect = Exception("Erro ao fechar conexão")
        self.conn = mock_conn
        
        # Verifica se a exceção é propagada
        with self.assertRaises(Exception) as context:
            self.close()
        
        # Verifica se a mensagem de erro está correta
        self.assertEqual(str(context.exception), "Erro ao fechar conexão")
        
        # Verifica se a conexão não foi definida como None em caso de erro
        self.assertIs(self.conn, mock_conn)
    
    # Executa todos os testes de fechamento
    test_fechamento_conexao_ativa()
    test_fechamento_sem_conexao()
    test_erro_ao_fechar()    