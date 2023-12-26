# pylint: skip-file
from unittest.mock import MagicMock, patch
import pytest
import requests
import pandas as pd
from sqlalchemy import text

from etl.downloader import Downloader
from etl.files import File
from etl.process import ETL

@pytest.fixture
def mock_download_object():
    mock_download_object = MagicMock(spec=Downloader)
    mock_download_object.file_path = 'test_file.txt'
    return mock_download_object


@pytest.fixture
def mock_file():
    return MagicMock(spec=File)


def test_create_etl_default_object():
    etl = ETL()
    assert etl.sleep_time == 0
    assert etl.file_handler == File


def test_create_etl_object(mock_file):
    etl = ETL(sleep_time=5, file_handler=mock_file)
    assert etl.sleep_time == 5
    assert etl.file_handler == mock_file


def test_extract_file_not_exists_mode_append(mock_download_object, mock_file):
    mock_download_object.download.return_value = 'example data'
    mock_file.return_value.exists.return_value = False
    mock_file.return_value.save.return_value = None
    test_queue = [mock_download_object]
    etl = ETL(file_handler=mock_file)
    return_objs = list(etl.extract(test_queue, mode='append'))

    assert len(return_objs) == 1
    mock_download_object.download.assert_called_once()
    handle = mock_file()
    handle.exists.assert_called_once()
    handle.save.assert_called_once_with('example data')


def test_extract_file_exists_mode_append(mock_download_object, mock_file):
    mock_download_object.download.return_value = 'example data'
    mock_file.return_value.exists.return_value = True
    mock_file.return_value.save.return_value = None
    test_queue = [mock_download_object]
    etl = ETL(file_handler=mock_file)
    return_objs = list(etl.extract(test_queue, mode='append'))

    assert len(return_objs) == 1
    mock_download_object.download.assert_not_called()
    handle = mock_file()
    handle.exists.assert_called_once()
    handle.save.assert_not_called()


def test_extract_file_exists_mode_replace(mock_download_object, mock_file):
    mock_download_object.download.return_value = 'example data'
    mock_file.return_value.exists.return_value = True
    mock_file.return_value.save.return_value = None
    test_queue = [mock_download_object]
    etl = ETL(file_handler=mock_file)
    return_objs = list(etl.extract(test_queue, mode='replace'))

    assert len(return_objs) == 1
    mock_download_object.download.assert_called_once()
    handle = mock_file()
    handle.exists.assert_called_once()
    handle.save.assert_called_once_with('example data')


def test_extract_on_error(mock_download_object, mock_file):
    mock_response = MagicMock(spec=requests.Response)
    mock_response.status_code = 404
    mock_download_object.download.return_value = 'example data'
    mock_download_object.url = 'test_url.com'
    mock_download_object.download.side_effect = requests.HTTPError(response=mock_response)
    mock_file.return_value.exists.return_value = True
    mock_file.return_value.save.return_value = None
    test_queue = [mock_download_object]
    etl = ETL(file_handler=mock_file)
    return_objs = list(etl.extract(test_queue, mode ='replace'))

    assert len(return_objs) == 0
    mock_download_object.download.assert_called_once()
    handle = mock_file()
    handle.exists.assert_called_once()
    handle.save.assert_not_called()
    

def test_extract_w_callback(mock_download_object, mock_file):
    def callback(content):
        if content == 'example data 2':
            return []
        other_mock_download_object = MagicMock(spec=Downloader)
        other_mock_download_object.file_path = 'test_file2.csv'
        other_mock_download_object.download.return_value = 'example data 2'

        return [other_mock_download_object]
    
    mock_download_object.download.return_value = 'example data'
    mock_file.return_value.exists.return_value = False
    mock_file.return_value.save.return_value = None
    test_queue = [mock_download_object]
    etl = ETL(file_handler=mock_file)
    return_objs = list(etl.extract(test_queue, mode='append', callback=callback))

    assert len(return_objs) == 2
    assert mock_download_object.download.call_count == 1
    handle = mock_file()
    assert handle.exists.call_count == 2
    assert handle.save.call_count == 2


def test_transform(mock_download_object, mock_file):
    mock_file.return_value.read.return_value = 'example data'
    mock_parser = MagicMock()
    mock_parser.parse.return_value = 'parsed_data'
    mock_validation_pipeline = MagicMock()
    mock_validation_pipeline.validate.return_value = None
    mock_transform_pipeline = MagicMock()
    mock_transform_pipeline.apply.return_value = 'parsed data'

    etl = ETL('mode', file_handler=mock_file)
    result = etl.transform(
        mock_download_object,
        parser=mock_parser,
        transform_pipeline=mock_transform_pipeline,
        validation_pipeline=mock_validation_pipeline
    )

    handle = mock_file()
    handle.read.assert_called_once()

    mock_parser.parse.assert_called_once_with('example data')
    mock_validation_pipeline.validate.assert_called_once_with('parsed_data')
    mock_transform_pipeline.apply.assert_called_once_with('parsed_data')

    assert result[0] == mock_download_object
    assert result[1] == 'parsed data'


def test_transform_only_data(mock_download_object, mock_file):
    mock_file.return_value.read.return_value = 'example data'
    mock_parser = MagicMock()
    mock_parser.parse.return_value = 'parsed_data'
    mock_validation_pipeline = MagicMock()
    mock_transform_pipeline = MagicMock()

    etl = ETL('append', file_handler=mock_file)
    result = etl.transform(mock_download_object)

    handle = mock_file()
    handle.read.assert_called_once()

    mock_parser.parse.assert_not_called()
    mock_validation_pipeline.validate.assert_not_called()
    mock_transform_pipeline.apply.assert_not_called()


def test_load_replace(mock_download_object):
    mock_download_object.table = 'test_table'
    mock_download_object.schema = 'test_schema'
    data = pd.DataFrame({'col1': [1, 2], 'col2': ['a', 'b']})
    mock_session = MagicMock()
    etl = ETL()

    with patch('pandas.DataFrame.to_sql') as mock_to_sql:
        etl.load((mock_download_object, data), mock_session, mode='replace')

    mock_session.execute.assert_called_once_with('DELETE FROM test_schema.test_table')
    mock_to_sql.assert_called_once_with('test_table', mock_session.bind, schema='test_schema', if_exists='append')


def test_load_append_mode(mock_download_object):
    mock_download_object.table = 'test_table'
    mock_download_object.schema = 'test_schema'
    data = pd.DataFrame({'col1': [1, 2], 'col2': ['a', 'b']})
    mock_session = MagicMock()

    etl = ETL()
    etl.load((mock_download_object, data), mock_session, mode='append')

    executed_query = mock_session.execute.call_args.args[0]
    expected_query = (
        'INSERT INTO test_schema.test_table (col1, col2) VALUES (:col1, :col2) '
        'ON CONFLICT DO UPDATE SET col1 = EXCLUDED.col1, col2 = EXCLUDED.col2'
    )

    assert str(executed_query) == expected_query
