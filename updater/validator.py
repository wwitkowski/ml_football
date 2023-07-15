"""Validators"""

from typing import Optional

import pandas as pd


class PandasDatasetValidator:
    """
    A class to validate Pandas data.

    Validated data is a pandas DataFrame.

    Attributes:
        config (dict): validation configuration

    Methods:
        validate(): Validate dataset
    """

    def __init__(self, config: dict) -> None:
        """
        Initalize PandasDataseTValidator class.

        Parameters:
            config (dict): validation configuration

        Returns:
            None
        """
        self.config = config

    def _validate_columns(self, data: pd.DataFrame, valid_columns: list[str]) -> None:
        """
        Validate DataFrame's columns. 
        
        All columns specified in configuration dictionary under the key "valid_columns" 
        must be present in the DataFrame. Otherwise AssertionError is raised

        Parameters:
            data (pd.DataFrame): data to be validated
            valid_columns (list[str]): list of columns that have to be in the DataFrame

        Returns:
            None
        """
        assert all((col in data.columns for col in valid_columns))

    def _validate_rows(
            self, 
            data: pd.DataFrame, 
            valid_columns: list[str], 
            threshold: Optional[float] = None
        ) -> pd.DataFrame:
        """
        Validate DataFrame's rows.

        If threshold specified in configuration, drop all rows with more na columns than allowed percentage.
        Drop all rows with na values for valid_colmns as well.

        Parameters:
            data (pd.DataFrame): data to be validated
            valid_columns (list[str]): list of columns that need to have non-na value
            threshold (float): percente of na values below which the rows is going to be dropped

        Returns:
            data (pd.DataFrame): Validated data
        """
        if threshold:
            data = data.dropna(thresh=int(threshold*len(data.columns)))
        data = data.dropna(subset=valid_columns)
        return data

    def _validate_dtypes(self, data: pd.DataFrame, valid_dtypes: dict) -> pd.DataFrame:
        """
        Validate DataFrame's dtypes.

        Check if column has correct dtype. It is mainly to check if the numeric values were 
        properly read and dont have unexpected values which can often result in non-numeric 
        dtype being assigned. If wrong dtype - convert to numeric.

        Parameters:
            data (pd.DataFrame): data to be validated
            valid_dtypes (dict): column-dtype pairs specifying correct dtype for a column

        Returns:
            data (pd.DataFrame): Validated data
        """
        for col, dtype in valid_dtypes.items():
            if col not in data.columns:
                continue
            if dtype in ('int64', 'float64'):
                data.loc[:, col] = pd.to_numeric(data[col], errors='coerce')
        return data

    def validate(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Validate DataFrame.

        Parameters:
            data (pd.DataFrame)

        Returns:
            data (pd.DataFrame): Validated DataFrame
        """
        valid_column = self.config['columns']['valid_columns']
        valid_dtypes = self.config['valid_dtypes']
        threshold = self.config['rows'].get('threshold')
        self._validate_columns(data, valid_column)
        data = self._validate_rows(data, valid_column, threshold)
        data = self._validate_dtypes(data, valid_dtypes)
        return data
