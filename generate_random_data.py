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

        

