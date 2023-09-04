"""Datasets"""

import logging
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Iterator, ParamSpec, Any
from urllib.error import HTTPError

import pandas as pd

from etl.downloader import CSVDataDownloader
from etl.files import CSVFile
from etl.preprocessing import parse_dates


P = ParamSpec("P")
logger = logging.getLogger(__name__)


class Dataset(ABC):

    @abstractmethod
    def download_data(self, latest_date: datetime): # pragma: no cover
        """Abstract download method"""
        pass


class FootballDataCoUK(Dataset):
    """
    Class for managing FootballData.co.uk dataset.
    
    Attributes:
        config: dict[str, Any]: Dataset configuration parameters
    
    Methods:
        download_data (latest_date): Download datasets data since latest_date
    """

    def __init__(self, config: dict[str, Any]) -> None:
        """
        Initialize class.
        
        Parameters:
            config (dict[str, Any]): Dataset configuration parameters

        Returns:
            None
        """
        self.config = config
        self._downloader = CSVDataDownloader()
        self._file_manager = CSVFile

    @staticmethod
    def _generate_seasons(start_date: datetime, end_date: datetime) -> Iterator[str]:
        """
        Yields the Fotballdata Co Uk seasons representations between given dates.

        Parameters:
            start_date (datetime): Start date of the date range
            end_date (datetime): End date of the date range

        Yields:
            season (str): Season string representation
        """
        for i in range(end_date.year - start_date.year + 3):
            year = start_date.replace(year=start_date.year + i - 1).strftime('%y')
            year_plus_one = start_date.replace(year=start_date.year + i).strftime('%y')
            yield f'{year}{year_plus_one}'

    def _is_valid_data(self, data: pd.DataFrame) -> bool:
        """
        Checks validation statements.

        Parameters:
            data (pd.DataFrame): Data to be validated

        Returns:
            is_valid (bool): Whether the data is valid
        """
        return all(
            (col in data.columns for col in self.config['validation']['columns_required'])
        )

    def _preprocess_data(self, data: pd.DataFrame, season: str) -> pd.DataFrame:
        """
        Run preprocessing methods.

        Parameters:
            data (pd.DataFrame): Data to be prreprocessed
            season (str): Football season taht the data refers to

        Returns:
            data (pd.DataFrame): Preprocessed data
        
        """
        columns_select = self.config['preprocessing']['columns_select']
        columns_rename = self.config['preprocessing']['columns_rename']
        columns_numeric = self.config['preprocessing']['columns_to_numeric']
        date_formats = self.config['preprocessing']['date_formats']
        date_column = self.config['preprocessing']['date_column']
        not_null_columns = self.config['preprocessing']['not_null_columns']

        data = (
            data[[col for col in data.columns if col in columns_select]].copy()
            .pipe(parse_dates, col=date_column, date_formats=date_formats)
            .dropna(subset=not_null_columns)
            .apply(
                lambda col: pd.to_numeric(col, errors='coerce') if col.name in columns_numeric else col, 
                axis=0
            )
            .rename(columns=columns_rename)
            .assign(season=season)
        )
        return data
        

    def download_data(self, latest_date: datetime) -> pd.DataFrame:
        """
        Download data and filter it since latest_date.

        Parameters:
            latest_date (datetime): Date to filter the data on

        Returns:
            data (pd.DataFrame): Downloaded and preprocessed valid data
        """
        dataframes = []
        for league in self.config['leagues']:
            for season in self._generate_seasons(latest_date, datetime.today().date()):
                filepath = f'data/{self.__class__.__name__}/{season}/{league}.csv'
                file = self._file_manager(filepath)
                if file.exists():
                    continue
                logger.info('DOWNLOADING: %s - %s', league, season)
                try:
                    raw_df = self._downloader.download(f"{self.config['base_url']}{season}/{league}.csv")
                except HTTPError as err:
                    if err.code in (300, 404):
                        logger.info('Data %s - %s does not exist.', league, season)
                        continue
                    else:
                        raise
                if not self._is_valid_data(raw_df):
                    logger.info('Data %s - %s is not valid.', league, season)
                    continue
                preprocessed_df = self._preprocess_data(raw_df, season)
                file.save(preprocessed_df, index=False)
                dataframes.append(preprocessed_df)
        if not dataframes:
            return pd.DataFrame()
        data = pd.concat(dataframes)
        data = data[data['match_date'].dt.date > latest_date]
        return data
