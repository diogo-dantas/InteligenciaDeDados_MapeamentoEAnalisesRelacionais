import pytest
import os
from dotenv import load_dotenv

@pytest.fixture(autouse=True)
def load_env():
    load_dotenv()
    
    if not os.getenv('DB_HOST'):
        os.environ['DB_HOST'] = 'localhost'
    if not os.getenv('DB_PORT'):
        os.environ['DB_PORT'] = '5432'
    if not os.getenv('DB_NAME'):
        os.environ['DB_NAME'] = 'test_db'
    if not os.getenv('DB_USER'):
        os.environ['DB_USER'] = 'postgres'
    if not os.getenv('DB_PASSWORD'):
        os.environ['DB_PASSWORD'] = 'postgres'