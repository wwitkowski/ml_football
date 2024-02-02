"""Date util functions"""
from datetime import datetime, timedelta
from typing import Iterator, Tuple, List

import pandas as pd

from etl.exceptions import DataParserException


def generate_seasons(start_date: datetime, end_date: datetime) -> Iterator[Tuple[str, str]]:
    """
    Yields the FootballData Co Uk seasons representations between given dates.

    Parameters:
        start_date (datetime): Start date of the date range
        end_date (datetime): End date of the date range

    Yields:
        Tuple[str, str]: Season representation in the format ('YY/YY', 'YYYY/YYYY')
    """
    for i in range(end_date.year - start_date.year + 2):
        year = start_date.replace(year=start_date.year + i - 1)
        year_plus_one = start_date.replace(year=start_date.year + i)
        yield (
            f"{year.strftime('%y')}{year_plus_one.strftime('%y')}", 
            f"{year.strftime('%Y')}/{year_plus_one.strftime('%Y')}"
        )


def generate_dates(start_date: datetime, end_date: datetime) -> Iterator[datetime]:
    """
    Yields dates between given dates.

    Parameters:
        start_date (datetime): Start date of the date range
        end_date (datetime): End date of the date range

    Yields:
        datetime: Date representation
    """
    for i in range(int((end_date - start_date).days) + 1):
        yield start_date + timedelta(i)


def parse_dataframe_dates(data: pd.DataFrame, col: str, date_formats: List[str]) -> pd.DataFrame:
    """
    Parse pandas dataframe column to datetime format.

    Parameters:
        data (pd.DataFrame): Data with column to be converted
        col (str): Date column name
        date_formats (List[str]): List of formats to try to match the date
    
    Returns:
        pd.DataFrame: Data with converted date columns
    
    Raises:
        DataParserError: If none of the date formats match the specified column's date format
    """
    data = data.copy()
    for date_format in date_formats:
        try:
            data[col] = pd.to_datetime(data[col], format=date_format)
            return data
        except ValueError:
            pass
    raise DataParserException(f'None of {date_formats} match {col} date format')
