# pylint: skip-file
from unittest.mock import MagicMock
import pytest
import requests
import pandas as pd
from etl.download_strategy import DownloadStrategy

from etl.downloader import Downloader
from etl.files import File
from etl.process import ETL


@pytest.fixture
def mock_file():
    mock_file = MagicMock(spec=File)
    mock_file.return_value.save.return_value = None
    mock_file.return_value.read.return_value = 'example data'
    return mock_file


@pytest.fixture
def mock_download_object(mock_file):
    mock_download_object = MagicMock(spec=Downloader)
    mock_download_object.file = mock_file()
    mock_download_object.table = 'test_table'
    mock_download_object.schema = 'test_schema'
    return mock_download_object


@pytest.fixture
def mock_strategy():
    return MagicMock(spec=DownloadStrategy)


def test_create_etl_default_object():
    etl = ETL()
    assert etl._queue == []
    assert etl.sleep_time == 0


def test_create_etl_object():
    etl = ETL(sleep_time=5)
    assert etl._queue == []
    assert etl.sleep_time == 5


def test_process_queue_download_required(mock_download_object, mock_strategy):
    mock_strategy.return_value.is_download_required.return_value = True
    test_queue = [mock_download_object]
    etl = ETL()
    return_objs = list(etl.process_queue(test_queue, strategy=mock_strategy()))

    assert len(return_objs) == 1
    assert len(etl._queue) == 0


def test_process_queue_download_not_required(mock_download_object, mock_strategy):
    mock_strategy.return_value.is_download_required.return_value = False
    test_queue = [mock_download_object]
    etl = ETL()
    return_objs = list(etl.process_queue(test_queue, strategy=mock_strategy()))

    assert len(return_objs) == 0
    assert len(etl._queue) == 0


def test_extract(mock_download_object):
    mock_download_object.download.return_value = 'example data'
    etl = ETL()
    return_obj = etl.extract(mock_download_object)

    assert return_obj == mock_download_object
    mock_download_object.download.assert_called_once_with(None)
    mock_download_object.file.save.assert_called_once()


def test_extract_w_session(mock_download_object):
    mock_download_object.download.return_value = 'example data'
    etl = ETL()
    mock_session = MagicMock(spec=requests.Session)
    return_obj = etl.extract(mock_download_object, session=mock_session)

    assert return_obj == mock_download_object
    mock_download_object.download.assert_called_once_with(mock_session)
    mock_download_object.file.save.assert_called_once()
    

def test_extract_w_callback(mock_download_object):
    def callback(content):
        return [mock_download_object]*2
    
    mock_download_object.download.return_value = 'example data'
    etl = ETL()
    return_obj = etl.extract(mock_download_object, callback=callback)

    assert return_obj == mock_download_object
    mock_download_object.download.assert_called_once_with(None)
    mock_download_object.file.save.assert_called_once()
    assert len(etl._queue) == 2


def test_transform(mock_download_object):
    mock_parser = MagicMock()
    mock_parser.parse.return_value = 'parsed_data'
    mock_validation_pipeline = MagicMock()
    mock_validation_pipeline.validate.return_value = None
    mock_transform_pipeline = MagicMock()
    mock_transform_pipeline.apply.return_value = 'parsed data'

    etl = ETL()
    result = etl.transform(
        mock_download_object,
        parser=mock_parser,
        transform_pipeline=mock_transform_pipeline,
        validation_pipeline=mock_validation_pipeline
    )

    assert result[0] == mock_download_object
    assert result[1] == 'parsed data'
    mock_parser.parse.assert_called_once_with('example data')
    mock_validation_pipeline.validate.assert_called_once_with('parsed_data')
    mock_transform_pipeline.apply.assert_called_once_with('parsed_data')



def test_transform_only_data(mock_download_object):
    mock_parser = MagicMock()
    mock_validation_pipeline = MagicMock()
    mock_transform_pipeline = MagicMock()

    etl = ETL()
    result = etl.transform(mock_download_object)

    assert result[0] == mock_download_object
    assert result[1] == 'example data'
    mock_parser.parse.assert_not_called()
    mock_validation_pipeline.validate.assert_not_called()
    mock_transform_pipeline.apply.assert_not_called()


def test_load_replace(mock_download_object):
    data = pd.DataFrame({'col1': [1, 2], 'col2': ['a', 'b']})
    mock_session = MagicMock()

    etl = ETL()
    etl.load((mock_download_object, data), mock_session, mode='replace')

    executed_query = mock_session.execute.call_args.args[0]
    expected_query = (
        'INSERT INTO test_schema.test_table (col1, col2) VALUES (:col1, :col2) '
        'ON CONFLICT ON CONSTRAINT test_table_unique DO UPDATE SET col1 = EXCLUDED.col1, col2 = EXCLUDED.col2'
    )

    assert str(executed_query) == expected_query


def test_load_append_mode(mock_download_object):
    data = pd.DataFrame({'col1': [1, 2], 'col2': ['a', 'b']})
    mock_session = MagicMock()

    etl = ETL()
    etl.load((mock_download_object, data), mock_session, mode='append')

    executed_query = mock_session.execute.call_args.args[0]
    expected_query = (
        'INSERT INTO test_schema.test_table (col1, col2) VALUES (:col1, :col2) '
        'ON CONFLICT ON CONSTRAINT test_table_unique DO NOTHING'
    )

    assert str(executed_query) == expected_query
