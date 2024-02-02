# pylint: skip-file
from click import File
import pytest
from unittest.mock import Mock
from etl.data_object import DataObject

from etl.downloader import Downloader
from etl.uploader import Uploader


@pytest.fixture
def mock_downloader():
    return Mock(spec=Downloader)


@pytest.fixture
def mock_file():
    return Mock(spec=File)


@pytest.fixture
def mock_uploader():
    return Mock(spec=Uploader)


def test_data_object_creation(mock_downloader, mock_file, mock_uploader):
    data_object = DataObject(downloader=mock_downloader, file=mock_file, uploader=mock_uploader)
    assert data_object.downloader == mock_downloader
    assert data_object.file == mock_file
    assert data_object.uploader == mock_uploader
    assert data_object.meta == {}


def test_data_object_creation_with_meta(mock_downloader, mock_file, mock_uploader):
    meta = {'key': 'value'}
    data_object = DataObject(downloader=mock_downloader, file=mock_file, uploader=mock_uploader, meta=meta)
    assert data_object.downloader == mock_downloader
    assert data_object.file == mock_file
    assert data_object.uploader == mock_uploader
    assert data_object.meta == meta
