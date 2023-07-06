import os
import logging
import sqlalchemy
from sqlalchemy import text


logger = logging.getLogger(__name__)

class PGDatabase:

    def __init__(self, host='localhost:8001', database='ml_football', user='postgres', password=os.getenv('POSTGRES_PASSWORD')):
        self.engine = self._create_engine(host, database, user, password)

    def _create_engine(self, host, database, user, password):
        logger.info('Creating DB engine..')
        return sqlalchemy.create_engine(f'postgresql+psycopg2://{user}:{password}@{host}/{database}')
    
    def execute(self, query):
        with self.engine.connect() as conn:
            logger.info('Executing query: %s', query)
            result = conn.execute(text(query))
        return result
    
    def fetch(self, query):
        return self.execute(query).fetchall()
