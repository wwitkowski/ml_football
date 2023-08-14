from pathlib import Path
import pytest
from unittest.mock import patch
import pandas as pd

from etl.files import CSVFile


@pytest.fixture
def mock_file():
    example_str_path = 'folder/file.csv'
    return CSVFile(example_str_path)

def test_create_file_object():
    example_str_path = 'folder/file.csv'
    expected_path = Path(example_str_path)
    
    file_str = CSVFile(example_str_path)
    assert file_str.path == expected_path

    file_path = CSVFile(expected_path)
    assert file_path.path == expected_path

def test_exists(mock_file):
    with patch("pathlib.Path.is_file") as mock_is_file:
        mock_is_file.return_value = False
        assert not mock_file.exists()

        mock_is_file.return_value = True
        assert mock_file.exists()

def test_read(mock_file):
    data = pd.DataFrame({"column1": [1, 2, 3], "column2": ["A", "B", "C"]})
    with patch("pandas.read_csv", return_value=data):
        read_data = mock_file.read()
        pd.testing.assert_frame_equal(data, read_data)

def test_save(mock_file):
    data = pd.DataFrame({"column1": [1, 2, 3], "column2": ["A", "B", "C"]})
    with patch("pandas.DataFrame.to_csv") as mock_to_csv:
        mock_file.save(data, index=False)
        mock_to_csv.assert_called_once_with(mock_file.path, index=False)