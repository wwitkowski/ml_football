"""Date utils"""

from datetime import datetime, timedelta
from typing import Iterator

import pandas as pd


def generate_seasons(start_date: datetime, end_date: datetime) -> Iterator[str]:
    """
    Yields the Fotballdata Co Uk seasons representations between given dates.

    Parameters:
        start_date (datetime): Start date of the date range
        end_date (datetime): End date of the date range

    Yields:
        season (str): Season string representation
    """
    for i in range(end_date.year - start_date.year + 3):
        year = start_date.replace(year=start_date.year + i - 1)
        year_plus_one = start_date.replace(year=start_date.year + i)
        yield (
            f"{year.strftime('%y')}{year_plus_one.strftime('%y')}", 
            f"{year.strftime('%Y')}{year_plus_one.strftime('%Y')}"
        )


def generate_dates(start_date: datetime, end_date: datetime) -> Iterator[str]:
    """
    Yields the dates in str format between given dates.

    Parameters:
        start_date (datetime): Start date of the date range
        end_date (datetime): End date of the date range

    Yields:
        date (str): Date string representation
    """
    for i in range(int((end_date - start_date).days)):
        yield start_date + timedelta(i)


def parse_dates(data: pd.DataFrame, col: str, date_formats: list[str]) -> pd.DataFrame:
    """
    Parse pandas dataframe column to datetime format.

    Parameters:
        data (pd.DataFrame): Data with column to be converted
        col (str): Date column name
        date_formats (list[str]): list of formats to try to match the date
    
    Returns:
        data (pd.DataFrame): data with converted date columns
    """
    data = data.copy()
    for date_format in date_formats:
        try:
            data[col] = pd.to_datetime(data[col], format=date_format)
            return data
        except ValueError:
            pass
    raise pd.errors.ParserError(f'None of {date_formats} match {col} date format')