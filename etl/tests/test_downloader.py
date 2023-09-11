# pylint: skip-file
from typing import ParamSpec
from unittest.mock import patch

import pytest
import pandas as pd

from etl.downloader import CSVRequestsDataDownloader


P = ParamSpec("P")


@pytest.fixture(autouse=True)
def patch_requests_get():
    """mock requests.get"""
    with patch('requests.Session.get') as mock_get:
        yield mock_get


def test_csv_downloader_init() -> None:
    encoding = 'utf-8'
    downloader = CSVRequestsDataDownloader(encoding=encoding)
    assert downloader.encoding == encoding


def test_csv_downloader_valid_csv(patch_requests_get) -> None:
    def correct_csv_content():
        content = [
            b'col1,col2',
            b'1,4',
            b'2,5',
            b'3,6'
        ]
        for line in content:
            yield line

    url = 'https://example.com/data.csv'
    expected_data = pd.DataFrame({'col1': ['1', '2', '3'], 'col2': ['4', '5', '6']})
    patch_requests_get.return_value.iter_lines.return_value = correct_csv_content()
    downloader = CSVRequestsDataDownloader()
    data = downloader.download(url)
    print(data)
    print(expected_data)
    pd.testing.assert_frame_equal(data.reset_index(drop=True), expected_data.reset_index(drop=True))


def test_csv_downloader_bad_csv(patch_requests_get) -> None:
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
    
    downloader = CSVRequestsDataDownloader(encoding='utf-8')
    data = downloader.download(url)
    pd.testing.assert_frame_equal(data.reset_index(drop=True), expected_data.reset_index(drop=True))
