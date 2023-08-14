"""Preprocessing helper functions"""

import pandas as pd

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