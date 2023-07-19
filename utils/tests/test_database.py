import pytest
import sqlalchemy
from unittest.mock import Mock, MagicMock, patch
from utils.database import Database, create_postgres_engine


CONN_CONF = {
    'user': 'testuser',
    'password': 'testpassword',
    'host': 'localhost',
    'database': 'testdb'
}


@pytest.fixture(scope='function')
def database():
    #engine = Mock(spec=sqlalchemy.create_engine('postgresql+psycopg2://{user}:{password}@{host}/{database}'.format(**CONN_CONF)))
    engine = Mock(spec=sqlalchemy.engine.Engine)
    database = Database(engine)
    return database


def test_create_postgres_engine_dict():
    engine = create_postgres_engine(CONN_CONF)
    assert str(engine.url) == 'postgresql+psycopg2://testuser:***@localhost/testdb'

def test_create_postgres_engine_string():
    engine = create_postgres_engine('postgresql+psycopg2://testuser:testpassword@localhost/testdb')
    assert str(engine.url) == 'postgresql+psycopg2://testuser:***@localhost/testdb'


def test_execute(database):
    result_mock = Mock()
    result_mock.fetchone.return_value = (1,)
    mock_conn = MagicMock()
    database.engine.connect.return_value = mock_conn
    mock_conn.__enter__.return_value.execute.return_value = result_mock
    result = database.execute("SELECT 1")
    assert result.fetchone() == (1,)
    


def test_fetch(database):
    result_mock = Mock()
    result_mock.fetchall.return_value = [(1, 'Alice'), (2, 'Bob')]
    database.execute = Mock(return_value=result_mock)

    result = database.fetch("SELECT * FROM test_table")
    assert result == [(1, 'Alice'), (2, 'Bob')]

