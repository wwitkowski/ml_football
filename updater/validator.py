"""Validators"""

from abc import ABC, abstractmethod

import pandas as pd


class DatasetValidator(ABC):
    """Abstract Validator class"""

    @abstractmethod
    def validate(self, data): # pragma: no cover
        pass


class PandasDatasetValidator(DatasetValidator):
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
        self._valid_columns = self.config.get('columns', {}).get('valid_columns', [])
        self._valid_dtypes = self.config.get('valid_dtypes', {})
        self._threshold = self.config.get('rows', {}).get('threshold')

    def _validate_columns(self, data: pd.DataFrame) -> None:
        """
        Validate DataFrame's columns. 
        
        All columns specified in configuration dictionary under the key "valid_columns" 
        must be present in the DataFrame. Otherwise AssertionError is raised

        Parameters:
            data (pd.DataFrame): data to be validated

        Returns:
            None
        """
        assert all((col in data.columns for col in self._valid_columns))

    def _validate_rows(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Validate DataFrame's rows.

        If threshold specified in configuration, drop all rows with more na columns than allowed percentage.
        Drop all rows with na values for valid_colmns as well.

        Parameters:
            data (pd.DataFrame): data to be validated

        Returns:
            data (pd.DataFrame): Validated data
        """
        if self._threshold:
            data = data.dropna(thresh=len(data.columns) - round(self._threshold*len(data.columns))).copy()
        if self._valid_columns:
            data = data.dropna(subset=self._valid_columns).copy()
        else:
            data = data.dropna(how='all').copy()
        return data

    def _validate_dtypes(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Validate DataFrame's dtypes.

        Check if column has correct dtype. It is mainly to check if the numeric values were 
        properly read and dont have unexpected values which can often result in non-numeric 
        dtype being assigned. If wrong dtype - convert to numeric.

        Parameters:
            data (pd.DataFrame): data to be validated

        Returns:
            data (pd.DataFrame): Validated data
        """
        specific_dtypes = {}
        for col, dtype in self._valid_dtypes.items():
            if dtype == 'numeric':
                data[col] = pd.to_numeric(data[col], errors='coerce')
            else:
                specific_dtypes[col] = dtype
        data = data.astype(specific_dtypes)
        return data

    def validate(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Validate DataFrame.

        Parameters:
            data (pd.DataFrame)

        Returns:
            data (pd.DataFrame): Validated DataFrame
        """
        self._validate_columns(data)
        data = self._validate_rows(data)

        if self._valid_dtypes:
            data = self._validate_dtypes(data)
        return data
