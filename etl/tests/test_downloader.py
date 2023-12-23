# pylint: skip-file
from unittest.mock import Mock, patch
import pytest
import requests
from etl.downloader import APIDownloader


@pytest.fixture
def mock_request():
    with patch('etl.downloader.requests.Session.request') as mock_session:
        yield mock_session


def test_create_downloader():
    downloader = APIDownloader(
        'GET',
        'http://test_url.com',
        'test_file.txt',
        'test_table',
        'test_schema',
        headers={'header': 'test'}
    )
    assert downloader.method == 'GET'
    assert downloader.url == 'http://test_url.com'
    assert downloader.file_path == 'test_file.txt'
    assert downloader.table == 'test_table'
    assert downloader.schema == 'test_schema'
    assert downloader.download_kwargs == {'headers': {'header': 'test'}}


def test_download(mock_request):
    downloader = APIDownloader(
        'GET',
        'http://test_url.com',
        'test_file.txt',
        'test_table',
        'test_schema',
        headers={'header': 'test'}
    )
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.content = b'Mock data content'
    mock_request.return_value = mock_response

    content = downloader.download()

    assert content == b'Mock data content'


def test_download_fail(mock_request):
    downloader = APIDownloader(
        'GET',
        'http://test_url.com',
        'test_file.txt',
        'test_table',
        'test_schema',
        headers={'header': 'test'}
    )
    mock_response = Mock()
    mock_response.status_code = 404
    mock_response.raise_for_status.side_effect = requests.HTTPError("404 Client Error")
    mock_request.return_value = mock_response

    with pytest.raises(requests.exceptions.HTTPError) as err:
        content = downloader.download()
