# pylint: skip-file
import pytest
from datetime import datetime
import pandas as pd
from etl.date_utils import generate_seasons, generate_dates, parse_dataframe_dates
from etl.exceptions import DataParserError  # Replace 'your_module_name' with the actual module name where the code resides


@pytest.fixture
def sample_data():
    return pd.DataFrame({
        'date_col': ['2021-01-01', '2022-02-02', '2023-03-03', '2024-04-04', '2025-05-05'],
        'other_col': [1, 2, 3, 4, 5]
    })


def test_generate_seasons():
    start_date = datetime(2020, 1, 1)
    end_date = datetime(2022, 12, 31)
    seasons = list(generate_seasons(start_date, end_date))
    assert seasons == [('1920', '2019/2020'), ('2021', '2020/2021'), ('2122', '2021/2022'), ('2223', '2022/2023')]


def test_generate_dates():
    start_date = datetime(2021, 1, 1)
    end_date = datetime(2021, 1, 4)
    dates = list(generate_dates(start_date, end_date))
    assert dates == [start_date, datetime(2021, 1, 2), datetime(2021, 1, 3), end_date]


def test_parse_dataframe_dates_with_valid_date_format(sample_data):
    date_formats = ['%Y-%m-%d']
    parsed_data = parse_dataframe_dates(sample_data, 'date_col', date_formats)
    assert isinstance(parsed_data['date_col'][0], pd.Timestamp)


def test_parse_dataframe_dates_with_invalid_date_format(sample_data):
    date_formats = ['%d-%m-%Y']
    with pytest.raises(DataParserError):
        parse_dataframe_dates(sample_data, 'date_col', date_formats)


def test_parse_dataframe_dates_with_no_matching_date_format(sample_data):
    date_formats = ['%d-%m-%Y', '%Y/%m/%d']
    with pytest.raises(DataParserError):
        parse_dataframe_dates(sample_data, 'date_col', date_formats)
