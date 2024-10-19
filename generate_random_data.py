import pandas as pd
import random
from datetime import datetime, timedelta
from faker import Faker
import psycopg2
from typing import Tuple, Optional
from dataclasses import dataclass


    """Configuração do banco de dados usando decorador para simplificação na criação da classe"""

@dataclass
class DbConfig:

	dbname: str
	user: str
	password: str
	host: str
	port: str

class DataGenerator:

   """Classe para geração e manipulação de dados sintéticos"""

   def __init__(self, db_config: DbConfig):
    self.db_config = db_config
    self.fake = Faker('pt_BR')
    Faker.seed(42)
    self.conn = None
    self.df_origem = None
    self.df_fluxo = None
    self.df_analises = None



    # Constantes
    self.TIPOS_SISTEMAS = [
    'ERP', 
    'CRM', 
    'E-commerce', 
    'App Mobile', 
    'Rede Social', 
    'IoT', 
    'Plataforma de Streaming', 
    'Banco de Dados em Nuvem', 
    'Sistema de Gestão de Projetos', 
    'Dispositivos Wearables', 
    'Plataforma de Análise de Dados', 
    'Marketplace'
    ]

    self.TIPOS_DADOS = [
    'transacional', 
    'log', 
    'analítico', 
    'sensor', 
    'mídia', 
    'documento'
    ]

    self.PADROES_LATENCIA = [
    'real-time', 
    'near real-time', 
    'batch', 
    'diário', 
    'semanal', 
    'mensal'
    ]

    self.DESTINOS = [
    'Data Warehouse', 
    'Data Lake', 
    'Business Intelligence', 
    'Machine Learning', 
    'Painéis Analíticos', 
    'API Gateway'
    ]

    self.STATUS = [
    'ativo', 
    'inativo', 
    'em manutenção', 
    'em teste'
    ]

    self.TIPOS_ANALISE = [
    'Análise de Tendências', 
    'Previsão de Demanda', 
    'Segmentação de Clientes',
    'Análise de Churn', 
    'Otimização de Processos', 
    'Detecção de Fraude',
    'Análise de Risco de Crédito', 
    'Análise de Satisfação do Cliente', 
    'Análise de Performance de Produtos', 
    'Análise de Comportamento de Transações', 
    'Análise de Preços de Serviços', 
    'Modelagem de Valor do Cliente'
    ]

    """Estabelecendo conexão com o banco de dados"""

    def connect(self) -> None:
    	try:
    		self.conn = psycopg2.connect(
    			dbname=self.db_config.dbname,
    			user=self.db_config.user,
    			password=self.db_config.password,
    			host=self.db_config.host,
    			port=self.db_config.port
    			)
    	except psycopg2.Error as e:
    		raise Exception(f"Erro ao conectar ao banco de dados: {e}")


    """Gerando dados randômicamente"""

    # tabela dados_origem

    def gerar_dados_origem(self, num_registros: int = 100) -> pd.DataFrame:
        data_base = datetime(2023, 1, 1)
        dados_origem = []

        for i in range(1, num_registros + 1):
            criado_em = data_base + timedelta(
                days=random.randint(0, 365),
                hours=random.randint(0, 23),
                minutes=random.randint(0, 59)
            )
            atualizado_em = criado_em + timedelta(
                days=random.randint(1, 30),
                hours=random.randint(0, 23),
                minutes=random.randint(0, 59)
            )

            sistema_base = random.choice(self.TIPOS_SISTEMAS)
            dados_origem.append({
              'id_origem': i,
              'nome_origem': f"Sistema {sistema_base} - {self.fake.company()}",
              'tipo_dado': random.choice(self.TIPOS_DADOS),
              'volume': random.randint(10000, 10000000),
              'latencia': random.choice(self.PADROES_LATENCIA),
              'descricao': self.fake.text(max_nb_chars=200)
              'criado_em': criado_em,
              'atualizado_em': atualizado_em
              })			

        self.df_origem = pd.DataFrame(dados_origem)
        return self.df_origem


    # tabela fluxo_dados

    def gerar_fluxo_dados(self, num_registros: int = 200) -> pd.DataFrame:
        if self.df_origem is None:
            raise ValueError("Execute gerar_dados_origem primeiro")

        fluxo_dados = []

        for i in range(1, num_registros + 1):
            data_criacao = atualizado_em + timedelta(
                days=random.randint(0, 30),
                hours=random.randint(0, 23),
                minutes=random.randint(0, 59)
            )
            data_atualizacao = data_criacao + timedelta(
                days=random.randint(0, 30),
                hours=random.randint(0, 23),
                minutes=random.randint(0, 59)
            )

            fluxo_dados.append({
                'id_fluxo': i,
                'id_origem': random.choice(self.df_origem['id_origem'].values),
                'destino': random.choice(self.DESTINOS),
                'status': random.choice(self.STATUS),
                'data_criacao': data_criacao,
                'data_atualizacao': data_atualizacao
            })
        
        self.df_fluxo = pd.DataFrame(fluxo_dados)
        return self.df_fluxo    

     # tabela gerar_analises

    def gerar_analises(self, num_registros: int = 300) -> pd.DataFrame:
        if self.df_fluxo is None:
            raise ValueError("Execute gerar_fluxo_dados primeiro")

        analises = []

        for i in range(1, num_registros + 1):
            tipo_analise = random.choice(self.TIPOS_ANALISE)
            fluxo_relacionado = self.df_fluxo.loc[random.choice(self.df_fluxo.index)]
            data_analise = fluxo_relacionado['data_criacao'] + \
                          timedelta(days=random.randint(1, 60))
            
            analises.append({
                'id_analise': i,
                'id_fluxo': fluxo_relacionado['id_fluxo'],
                'hipoteses': f"Hipótese: {tipo_analise} - {self.fake.sentence()}",
                'resultado': self.fake.text(max_nb_chars=200),
                'data_analise': data_analise,
                'responsavel': self.fake.name()
            })
        
        self.df_analises = pd.DataFrame(analises)
        return self.df_analises
   
    """Inserindo dados no banco"""

    def inserir_dados_no_banco(self) -> None:
        if not all([self.df_origem is not None, 
                   self.df_fluxo is not None, 
                   self.df_analises is not None]):
            raise ValueError("Gere todos os dados antes de inserir no banco")

        if self.conn is None:
            self.connect()

        try:
            with self.conn.cursor() as cursor:
                for _, row in self.df_origem.iterrows():
                    cursor.execute("""
                        INSERT INTO DadosOrigem (id_origem, nome_origem, tipo_dado, volume, latencia, descricao,criado_em, atualizado_em)
                        VALUES (%s, %s, %s, %s, %s, %s)
                    """, (row['id_origem'], row['nome_origem'], row['tipo_dado'],
                          row['volume'], row['latencia'], row['descricao'], row['criado_em'], row ['atualizado_em']))

                for _, row in self.df_fluxo.iterrows():
                    cursor.execute("""
                        INSERT INTO FluxoDados (id_fluxo, id_origem, destino, status, data_criacao, data_atualizacao)
                        VALUES (%s, %s, %s, %s, %s, %s)
                    """, (row['id_fluxo'], row['id_origem'], row['destino'],
                          row['status'], row['data_criacao'], row['data_atualizacao']))
                          
                for _, row in self.df_analises.iterrows():
                    cursor.execute("""
                        INSERT INTO Analises (id_analise, id_fluxo, hipoteses, resultado, data_analise, responsavel)
                        VALUES (%s, %s, %s, %s, %s, %s)
                    """, (row['id_analise'], row['id_fluxo'], row['hipoteses'],
                          row['resultado'], row['data_analise'], row['responsavel']))

            self.conn.commit()
        except Exception as e:
            self.conn.rollback()
            raise Exception(f"Erro ao inserir dados: {e}")

    """ Executa todo o processo de geração e inserção de dados, retorna tupla com os 3 dataframes gerados """

    def gerar_e_inserir_dados(self, 
                             num_origem: int = 100, 
                             num_fluxo: int = 200, 
                             num_analises: int = 300) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    
        self.gerar_dados_origem(num_origem)
        self.gerar_fluxo_dados(num_fluxo)
        self.gerar_analises(num_analises)
        self.inserir_dados_no_banco()

        return self.df_origem, self.df_fluxo, self.df_analises

    """ Fechando a conexão com o banco de dados """     

    def close(self) -> None:
        if self.conn is not None:
            self.conn.close()
            self.conn = None

    def __enter__(self):
        """Permite uso do context manager (with)"""
        self.connect()
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Fecha conexão ao sair do context manager"""
        self.close()