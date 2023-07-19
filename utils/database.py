"""Database connection classes"""

import logging
import sqlalchemy
from sqlalchemy import text
from typing import Any


logger = logging.getLogger(__name__)


def create_postgres_engine(conn_conf: dict | str) -> sqlalchemy.Engine:
    if isinstance(conn_conf, dict):
        conn_str = 'postgresql+psycopg2://{user}:{password}@{host}/{database}'.format(**conn_conf)
    else:
        conn_str = conn_conf
    return sqlalchemy.create_engine(conn_str)


class Database:
    """
    Class wrapping the Postgres connection and query executions.

    Attributes:
        engine (Engine): Connection engine.

    Methods:
        execute(query: str): Execute query.
        fetch(query: str): Fetch data from database.
    """

    def __init__(self, engine: sqlalchemy.Engine) -> None:
        """
        Initialize PGDatabase class. Create connection engine.

        Parameters:
            engine (sqlalchemy.Engine): Database engine

        Returns:
            None
        """
        self.engine = engine

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

    def fetch(self, query: str) -> list[tuple]:
        """
        Fetch data from database using provided query.

        Parameters:
            query (str): Query used to fetch data

        Returns:
            result (list[tuple]): Fetched data
        """
        return self.execute(query).fetchall()
