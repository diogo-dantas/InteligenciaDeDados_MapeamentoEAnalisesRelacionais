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

        		