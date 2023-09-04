# pylint: skip-file
from typing import ParamSpec
from unittest.mock import patch

import pytest
import pandas as pd

from etl.downloader import CSVDataDownloader



P = ParamSpec("P")


@pytest.fixture(autouse=True)
def patch_read_csv():
    """mock pandas.read_csv"""
    with patch('pandas.read_csv') as mock_read_csv:
        yield mock_read_csv


@pytest.fixture(autouse=True)
def patch_requests_get():
    """mock requests.get"""
    with patch('requests.get') as mock_get:
        yield mock_get


def test_csv_downloader_init() -> None:
    encoding = 'utf-8'
    downloader = CSVDataDownloader(encoding=encoding)
    assert downloader.encoding == encoding


def test_csv_downloader_valid_csv(patch_read_csv) -> None:
    url = 'https://example.com/data.csv'
    expected_data = pd.DataFrame({'col1': [1, 2, 3], 'col2': [4, 5, 6]})
    patch_read_csv.return_value = expected_data
    downloader = CSVDataDownloader()
    data = downloader.download(url)
    pd.testing.assert_frame_equal(data.reset_index(drop=True), expected_data.reset_index(drop=True))


def test_csv_downloader_bad_csv(patch_requests_get, patch_read_csv) -> None:
    def bad_csv_content():
        content = [
            b'col1,col2,col3',
            b'1,2,3',
            b'4,5,6,7',
            b',,',
            b''
        ]
        for line in content:
            yield line

    url = 'https://example.com/data.csv'
    
    expected_data = pd.DataFrame({'col1': ['1', '4'], 'col2': ['2', '5'], 'col3': ['3', '6']})
    patch_requests_get.return_value.iter_lines.return_value = bad_csv_content()
    patch_read_csv.side_effect = pd.errors.ParserError()
    
    downloader = CSVDataDownloader(encoding='utf-8')
    data = downloader.download(url)
    pd.testing.assert_frame_equal(data.reset_index(drop=True), expected_data.reset_index(drop=True))
