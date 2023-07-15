"""Database connection classes"""

import logging
import sqlalchemy
from sqlalchemy import text
from typing import Any


logger = logging.getLogger(__name__)


class PGDatabase:
    """
    Class wrapping the Postgres connection and query executions.

    Attributes:
        engine (Engine): Connection engine.

    Methods:
        execute(query: str): Execute query.
        fetch(query: str): Fetch data from database.
    """

    def __init__(self, host: str, database: str, user: str, password: str) -> None:
        """
        Initialize PGDatabase class. Create connection engine.

        Parameters:
            host (str): Database hostname
            database (str): Database name
            user (str): Database username used to connect
            password (str): Database username's password

        Returns:
            None
        """
        self.engine = self._create_engine(host, database, user, password)

    def _create_engine(self, host: str, database: str, user: str, password: str) -> sqlalchemy.Engine:
        """
        Create SQLAlchemy connection engine.

        Parameters:
            host (str): Database hostname
            database (str): Database name
            user (str): Database username used to connect
            password (str): Database username's password
        
        Returns:
            None
        """
        logger.info('Creating DB engine..')
        return sqlalchemy.create_engine(f'postgresql+psycopg2://{user}:{password}@{host}/{database}')

    def execute(self, query: str) -> Any:
        """
        Execute query and return the result.

        Parameters:
            query (str): Query that is going to be executed

        Returns:
            result (sqlalchemy.CursorResult): Result of executing the query
        """
        with self.engine.connect() as conn:
            logger.info('Executing query: %s', query)
            result = conn.execute(text(query))
        return result

    def fetch(self, query: str) -> dict:
        """
        Fetch data from database using provided query.

        Parameters:
            query (str): Query used to fetch data

        Returns:
            result (dict): Fetched data
        """
        return self.execute(query).fetchall()
