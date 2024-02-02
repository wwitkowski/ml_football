# pylint: skip-file
from unittest.mock import MagicMock, patch
import pytest
from etl.download_strategy import AppendStrategy, ReplaceStrategy
from etl.downloader import Downloader

from etl.files import File



@pytest.fixture
def mock_download_object():
    mock_download_object = MagicMock(spec=Downloader)
    mock_download_object.file = File('test_path.txt')
    return mock_download_object


def test_append_strategy(mock_download_object):
    strategy = AppendStrategy()
        
    with patch('etl.files.File.exists', return_value=True):
        result = strategy.is_download_required(mock_download_object)
        assert result == False

    with patch('etl.files.File.exists', return_value=False):
        result = strategy.is_download_required(mock_download_object)
        assert result == True


def test_replace_strategy(mock_download_object):
    strategy = ReplaceStrategy()
        
    with patch('etl.files.File.exists', return_value=False):
        result = strategy.is_download_required(mock_download_object)
        assert result == True

    with patch('etl.files.File.exists', return_value=True):
        result = strategy.is_download_required(mock_download_object)
        assert result == True
