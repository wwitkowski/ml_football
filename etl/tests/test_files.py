# pylint: skip-file
from pathlib import Path
from unittest.mock import mock_open, patch
import pytest

from etl.files import File


@pytest.fixture
def mock_file() -> File:
    example_str_path = 'folder/file.csv'
    return File(example_str_path)


def test_create_file_object() -> None:
    example_str_path = 'folder/file.csv'
    expected_path = Path(example_str_path)

    file_str = File(example_str_path)
    assert file_str.path == expected_path

    file_path = File(expected_path)
    assert file_path.path == expected_path


def test_exists(mock_file: File) -> None:
    with patch("pathlib.Path.is_file") as mock_is_file:
        mock_is_file.return_value = False
        assert not mock_file.exists()

        mock_is_file.return_value = True
        assert mock_file.exists()


def test_save(mock_file):
    with patch('etl.files.open', mock_open(), create=False) as m:
        with patch('pathlib.Path.mkdir'):
            mock_file.save('example data', mode='w')

    m.assert_called_once_with(Path('folder/file.csv'), 'w', encoding='utf-8')
    handle = m()
    handle.write.assert_called_once_with('example data')


def test_read(mock_file):
    with patch('etl.files.open', mock_open(read_data=b'example data'), create=False) as m:
        test_content = mock_file.read(mode='rb')

    m.assert_called_once_with(Path('folder/file.csv'), 'rb', encoding=None)
    assert test_content == b'example data'


def test_invalid_read_mode(mock_file):
    with pytest.raises(NotImplementedError):
        mock_file.read('r')


def test_save_failure(mock_file):
    with patch('etl.files.open', mock_open(), create=False) as m:
        with patch('pathlib.Path.mkdir'):
            handle = m()
            handle.write.side_effect = IOError()
            with pytest.raises(IOError):
                mock_file.save('example data')


def test_read_failure(mock_file):
    with patch('etl.files.open', mock_open(read_data=b'example data'), create=False) as m:
        handle = m()
        handle.read.side_effect = IOError()
        with pytest.raises(IOError):
            mock_file.read()


def test_read_no_file(mock_file):
    with pytest.raises(FileNotFoundError):
        mock_file.read()

