# pylint: skip-file
from unittest.mock import MagicMock
import pytest

from etl.data_object import DataObject
from etl.download_strategy import AppendStrategy
from etl.downloader import Downloader
from etl.files import File
from etl.process import ETL
from etl.uploader import Uploader


@pytest.fixture
def mock_file():
    mock_file = MagicMock(spec=File)
    mock_file.save.return_value = None
    mock_file.read.return_value = b'example_data'
    return mock_file


@pytest.fixture
def mock_downloader():
    mock_downloader = MagicMock(spec=Downloader)
    mock_downloader.download.return_value = 'example data'
    return mock_downloader

@pytest.fixture
def mock_uploader():
    mock_uploader = MagicMock(spec=Uploader)
    mock_uploader.upload.return_value = None
    return mock_uploader


@pytest.fixture
def mock_data_object():
    mock_data_object = MagicMock(spec=DataObject)
    return mock_data_object


def test_create_etl_object():
    etl = ETL()
    assert isinstance(etl.strategy, AppendStrategy)


def test_process_queue(mock_data_object):
    test_queue = [mock_data_object]*3
    etl = ETL()
    return_objs = list(etl.process_queue(test_queue))
    assert len(return_objs) == 3
    assert len(etl._queue) == 0


def test_process_queue_reverse(mock_data_object, mock_downloader, mock_file, mock_uploader):
    mock_data_object_last = DataObject(
        downloader=mock_downloader,
        file=mock_file,
        uploader=mock_uploader,
        meta={'id': 3},
    )
    test_queue = [mock_data_object]*2 + [mock_data_object_last]
    etl = ETL()
    return_objs = list(etl.process_queue(test_queue, reverse=True))
    assert return_objs[0].meta == {'id': 3}
    assert len(return_objs) == 3
    assert len(etl._queue) == 0


def test_process_queue_limited(mock_data_object):
    test_queue = [mock_data_object]*3
    etl = ETL()
    return_objs = list(etl.process_queue(test_queue, limit=2))
    assert len(return_objs) == 2


def test_extract(mock_data_object, mock_downloader, mock_file):
    mock_data_object.downloader = mock_downloader 
    mock_file.exists.return_value = False
    mock_data_object.file = mock_file

    etl = ETL()
    return_obj = etl.extract(mock_data_object)

    assert return_obj == mock_data_object
    mock_data_object.downloader.download.assert_called_once()
    mock_data_object.file.save.assert_called_once()


def test_extract_w_callback(mock_data_object, mock_downloader, mock_file):
    def callback(obj):
        return [mock_data_object]*2
    
    mock_data_object.downloader = mock_downloader 
    mock_file.exists.return_value = True
    mock_data_object.file = mock_file
    etl = ETL()
    return_obj = etl.extract(mock_data_object, callback=callback)

    assert return_obj == mock_data_object
    mock_data_object.downloader.download.assert_not_called()
    mock_data_object.file.save.assert_not_called()
    assert len(etl._queue) == 2


def test_transform(mock_data_object, mock_file):
    mock_parser = MagicMock()
    mock_parser.parse.return_value = 'parsed_data'
    mock_validation_pipeline = MagicMock()
    mock_validation_pipeline.validate.return_value = None
    mock_transform_pipeline = MagicMock()
    mock_transform_pipeline.apply.return_value = 'parsed data'
    mock_data_object.file = mock_file

    etl = ETL()
    result = etl.transform(
        mock_data_object,
        parser=mock_parser,
        transform_pipeline=mock_transform_pipeline,
        validation_pipeline=mock_validation_pipeline
    )

    assert result[0] == mock_data_object
    assert result[1] == 'parsed data'
    mock_data_object.file.read.assert_called_once()
    mock_parser.parse.assert_called_once_with(b'example_data')
    mock_validation_pipeline.validate.assert_called_once_with('parsed_data')
    mock_transform_pipeline.apply.assert_called_once_with('parsed_data')


def test_transform_only_data(mock_data_object, mock_file):
    mock_parser = MagicMock()
    mock_validation_pipeline = MagicMock()
    mock_transform_pipeline = MagicMock()
    mock_data_object.file = mock_file

    etl = ETL()
    result = etl.transform(mock_data_object)

    assert result[0] == mock_data_object
    assert result[1] == b'example_data'
    mock_data_object.file.read.assert_called_once()
    mock_parser.parse.assert_not_called()
    mock_validation_pipeline.validate.assert_not_called()
    mock_transform_pipeline.apply.assert_not_called()


def test_load(mock_data_object, mock_uploader):
    data = 'parsed data'
    mock_session = MagicMock()
    mock_data_object.uploader = mock_uploader
    etl = ETL()
    etl.load(mock_data_object, data, mock_session)

    mock_data_object.uploader.upload.assert_called_once_with(mock_session, 'parsed data')
