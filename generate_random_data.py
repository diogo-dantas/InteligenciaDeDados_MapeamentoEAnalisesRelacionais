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
