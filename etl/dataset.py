"""Datasets"""

import logging
import time
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Iterator, ParamSpec, Any
from requests.exceptions import HTTPError

import pandas as pd
import numpy as np

from etl.downloader import CSVRequestsDataDownloader
from etl.files import CSVFile
from etl.preprocessing import parse_dates


P = ParamSpec("P")
logger = logging.getLogger(__name__)


class Dataset(ABC):
    """Abstract Dataset class"""

    @abstractmethod
    def download_data(self, latest_date: datetime) -> Any: # pragma: no cover
        """Abstract download method"""


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
        self._downloader = CSVRequestsDataDownloader(encoding='unicode_escape')
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
            .replace('', np.nan)
            .dropna(subset=not_null_columns)
            .apply(
                lambda col: pd.to_numeric(col, errors='coerce') if col.name in columns_numeric else col,
                axis=0
            )
            .rename(columns=columns_rename)
            .assign(season=season)
        )
        return data

    def download_data(self, latest_date: datetime, reload: bool = False) -> pd.DataFrame:
        """
        Download data and filter it since latest_date.

        Parameters:
            latest_date (datetime): Date to filter the data on
            reload (bool): Whether to redownload all data

        Returns:
            data (pd.DataFrame): Downloaded and preprocessed valid data
        """
        dataframes = []
        start_date = latest_date or datetime.strptime(self.config['default_start_date'], "%Y-%m-%d").date()
        for league in self.config['leagues']:
            for season in self._generate_seasons(start_date, datetime.today().date()):
                filepath = f'data/{self.__class__.__name__}/{season}/{league}.csv'
                file = self._file_manager(filepath)
                if file.exists() and not reload:
                    logger.info('Reading data from file')
                    df = file.read()
                    preprocessed_df = self._preprocess_data(df, season)
                    dataframes.append(preprocessed_df)
                    continue
                logger.info('DOWNLOADING: %s - %s', league, season)
                try:
                    raw_df = self._downloader.download(f"{self.config['base_url']}{season}/{league}.csv")
                except HTTPError as err:
                    if err.response.status_code in (300, 404):
                        logger.info('Data %s - %s does not exist.', league, season)
                        continue
                    raise
                if not self._is_valid_data(raw_df):
                    logger.info('Data %s - %s is not valid.', league, season)
                    continue
                file.save(raw_df, index=False)
                preprocessed_df = self._preprocess_data(raw_df, season)
                dataframes.append(preprocessed_df)
                time.sleep(2)
        if not dataframes:
            return pd.DataFrame()
        data = pd.concat(dataframes)
        data = data[data['match_date'].dt.date > start_date]
        return data
