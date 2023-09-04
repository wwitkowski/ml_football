import pytest
import pandas as pd
from unittest.mock import MagicMock, Mock, patch
from etl.update import ETL, run_etl
from etl.dataset import Dataset
from etl.merging import DataMerger


# Mock the Dataset class
class MockDataset(Dataset):
    def download_data(self, latest_date):
        # Return a sample DataFrame for testing
        return pd.DataFrame({'column1': [1, 2, 3]})


@pytest.fixture
def mock_session():
    mock_session = MagicMock()
    mock_session.execute.return_value.fetchone.return_value = ('2022-01-01',)
    return mock_session


def test_get_last_processed_date(mock_session):
    etl = ETL(mock_session)
    last_processed_date = etl.get_last_processed_date()
    print(last_processed_date)
    assert last_processed_date == '2022-01-01'


def test_add_dataset():
    etl = ETL(mock_session)
    dataset = MockDataset()
    etl.add_dataset(dataset)
    assert len(etl.datasets) == 1
    assert etl.datasets[0] == dataset


def test_run(mock_session):
    with patch('pandas.DataFrame.to_sql') as mock_to_sql:
        mock_to_sql.return_value = None
        mock_session.bind = None
        etl = ETL(mock_session)
        dataset = MockDataset()
        empty_dataset = Mock()
        empty_dataset.download_data.return_value = pd.DataFrame()
        etl.add_dataset(dataset)
        etl.add_dataset(empty_dataset)
        etl.run()

    assert mock_to_sql.called


def test_run_etl(mock_session):
    with (
        patch('yaml.safe_load') as mock_yaml_load, 
        patch('etl.update.ETL.run') as mock_etl,
    ):
        mock_session.return_value.__enter__.return_value = mock_session()
        mock_etl.return_value = None
        mock_yaml_load.return_value = {'some_config': 'value'}

        run_etl()

    assert mock_etl.called