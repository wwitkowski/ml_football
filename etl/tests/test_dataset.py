import pytest
from datetime import datetime
from unittest.mock import Mock, patch
from urllib.error import HTTPError

import numpy as np
import pandas as pd

from etl.dataset import FootballDataCoUK
from etl.files import File


valid_df = pd.DataFrame({
    'column1': ['1', '2', '3', '4', np.nan],
    'column2': ['A', 'B', 'C', 'D', np.nan],
    'column3': ['1', '2', '3', '4', np.nan],
    'match_date': ['2023-08-01', '2023-08-02', '2023-08-03', np.nan, '2023-08-05', ]
})


invalid_df = pd.DataFrame({
    'column2': ['A', 'B', 'C', 'D', np.nan],
    'column3': ['1', '2', '3', '4', np.nan],
    'match_date': ['2023-08-01', '2023-08-02', '2023-08-03', '2023-08-04', np.nan]
})


config = {
    'base_url': 'https://example.com/',
    'leagues': ['EPL'],
    'validation': {
        'columns_required': ['column1', 'match_date']
    },
    'preprocessing': {
        'columns_select': ['column1', 'column2', 'match_date'], 
        'columns_rename': {'column1': 'col1'}, 
        'columns_to_numeric': ['column1'],
        'date_formats': ['%Y-%m-%d'], 
        'date_column': 'match_date', 
        'not_null_columns': ['match_date'], 
    }
}


class MockFileExists(File):

    def __init__(self, filepath):
        self.filepath = filepath

    def exists(self):
        return True
    
    def read(self, **kwargs):
        pass
    
    def save(self, data, **kwargs):
        pass


class MockFileNotExists(File):

    def __init__(self, filepath):
        self.filepath = filepath

    def exists(self):
        return False
    
    def read(self, **kwargs):
        pass
    
    def save(self, data, **kwargs):
        pass


@pytest.fixture
def mock_data_downloader():
    mock = Mock()
    return mock


def test_download_data_valid_df(mock_data_downloader):
    expected_df = pd.DataFrame({
        'col1': [2, 3, np.nan],
        'column2': ['B', 'C', np.nan],
        'match_date': [datetime(2023, 8, 2), datetime(2023, 8, 3), datetime(2023, 8, 5)],
        'season': ['2324', '2324', '2324']
    })
    latest_date = datetime(2023, 8, 1)
    mock_data_downloader.download.return_value = valid_df

    football_data = FootballDataCoUK(config)
    football_data._downloader = mock_data_downloader
    football_data._file_manager = MockFileNotExists

    result = football_data.download_data(latest_date)
    pd.testing.assert_frame_equal(result.reset_index(drop=True), expected_df.reset_index(drop=True))


def test_download_data_invalid_df(mock_data_downloader):
    latest_date = datetime(2023, 8, 1)
    mock_data_downloader.download.return_value = invalid_df

    football_data = FootballDataCoUK(config)
    football_data._downloader = mock_data_downloader
    football_data._file_manager = MockFileNotExists

    result = football_data.download_data(latest_date)
    pd.testing.assert_frame_equal(result.reset_index(drop=True), pd.DataFrame().reset_index(drop=True))


def test_download_data_ok_http_err(mock_data_downloader):
    latest_date = datetime(2023, 8, 1)
    mock_data_downloader.download.side_effect = HTTPError(url='https://example.com/', code=404, msg='msg', hdrs=None, fp=None)

    football_data = FootballDataCoUK(config)
    football_data._downloader = mock_data_downloader
    football_data._file_manager = MockFileNotExists

    result = football_data.download_data(latest_date)
    pd.testing.assert_frame_equal(result.reset_index(drop=True), pd.DataFrame().reset_index(drop=True))


def test_download_data_nok_http_err(mock_data_downloader):
    latest_date = datetime(2023, 8, 1)
    mock_data_downloader.download.side_effect = HTTPError(url='https://example.com/', code=402, msg='msg', hdrs=None, fp=None)

    football_data = FootballDataCoUK(config)
    football_data._downloader = mock_data_downloader
    football_data._file_manager = MockFileNotExists

    with pytest.raises(HTTPError) as http_err:
        result = football_data.download_data(latest_date)
    

def test_download_data_file_exists():
    latest_date = datetime(2023, 8, 1)

    football_data = FootballDataCoUK(config)
    football_data._file_manager = MockFileExists

    result = football_data.download_data(latest_date)
    pd.testing.assert_frame_equal(result.reset_index(drop=True), pd.DataFrame().reset_index(drop=True))
