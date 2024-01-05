from unittest.mock import MagicMock, patch
import pytest
from etl.download_strategy import AppendStrategy, ReplaceOnMetaFlagStrategy, ReplaceStrategy
from etl.downloader import Downloader

from etl.files import File



@pytest.fixture
def mock_file():
    return MagicMock(spec=File)


@pytest.fixture
def mock_download_object():
    mock_download_object = MagicMock(spec=Downloader)
    mock_download_object.file_path = 'test_file.txt'
    return mock_download_object


def test_append_strategy(mock_file, mock_download_object):
    strategy = AppendStrategy()
        
    mock_file.return_value.exists.return_value = True
    result = strategy.is_download_required(mock_download_object, mock_file(mock_download_object))
    assert result == False

    mock_file.return_value.exists.return_value = False
    result = strategy.is_download_required(mock_download_object, mock_file(mock_download_object))
    assert result == True


def test_replace_strategy(mock_file, mock_download_object):
    strategy = ReplaceStrategy()
        
    mock_file.return_value.exists.return_value = True
    result = strategy.is_download_required(mock_download_object, mock_file(mock_download_object))
    assert result == True

    mock_file.return_value.exists.return_value = False
    result = strategy.is_download_required(mock_download_object, mock_file(mock_download_object))
    assert result == True


def test_replaceonmetaflag_strategy(mock_file, mock_download_object):
    strategy = ReplaceOnMetaFlagStrategy()

    mock_file.return_value.exists.return_value = False
    mock_download_object.return_value.meta = {'replace': True}
    result = strategy.is_download_required(mock_download_object(), mock_file(mock_download_object))
    assert result == True

    mock_file.return_value.exists.return_value = False
    mock_download_object.return_value.meta = {'replace': True}
    result = strategy.is_download_required(mock_download_object(), mock_file(mock_download_object))
    assert result == True

    mock_file.return_value.exists.return_value = True
    mock_download_object.return_value.meta = {'replace': True}
    result = strategy.is_download_required(mock_download_object(), mock_file(mock_download_object))
    assert result == True

    mock_file.return_value.exists.return_value = True
    mock_download_object.return_value.meta = {'replace': False}
    result = strategy.is_download_required(mock_download_object(), mock_file(mock_download_object))
    assert result == False
