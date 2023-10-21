"""Datasets"""

import logging
import time
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any
import requests

import pandas as pd
from etl.data_parser import CSVDataParser
from etl.data_quality import DataQualityValidator
from etl.date_utils import generate_seasons, parse_dates
from etl.download_processor import DownloadProcessor

from etl.downloader import URLDataDownloader
from etl.exceptions import NotValidDataException
from etl.files import CSVFileManager
from etl.preprocessing import PreprocessingPipeline


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
        config (dict[str, Any]): Dataset configuration parameters
    
    Methods:
        download_data(latest_date): Download datasets data since latest_date
    """

    def __init__(self, config: dict[str, Any]) -> None:
        """
        Initialize class.
        
        Parameters:
            config (Dict[str, Any]): Dataset configuration parameters

        Returns:
            None
        """
        self.dataframes = []
        self.config = config
        self.base_pipeline = self._get_preprocessing_pipeline(config['preprocessing'])
        self.parser = CSVDataParser(encoding='unicode_escape')

    @staticmethod
    def _get_preprocessing_pipeline(preprocessing_config):
        return (
            PreprocessingPipeline()
            .add_operation(pd.DataFrame.rename, **preprocessing_config['rename'])
            .add_operation(
                lambda df: df[[col for col in df.columns if col in preprocessing_config['columns_select']]])
            .add_operation(parse_dates, **preprocessing_config['parse_dates'])
            .add_operation(pd.DataFrame.replace, **preprocessing_config['replace'])
            .add_operation(pd.DataFrame.dropna, **preprocessing_config['dropna'])
            .add_operation(
                pd.DataFrame.apply, 
                lambda col: pd.to_numeric(col, errors='coerce') 
                if col.name in preprocessing_config['columns_to_numeric'] else col,
                axis=0
            )
            .add_operation(pd.DataFrame.convert_dtypes, **preprocessing_config['convert_dtypes'])   
        )
        
    @staticmethod
    def _get_validator(validation_config):
        return (
            DataQualityValidator()
                .add_condition(
                    lambda df: all(col in df.columns for col in validation_config['columns_required']), True
                )
        )

    def _download_seasonal_data(self, start_date, download_processor):
        for league in self.config['seasonal_dataset']['leagues']:
            for season_url, season in generate_seasons(start_date, datetime.today().date()):
                filepath = f'data/{self.__class__.__name__}/{season}/{league}.csv'
                url = f"{self.config['seasonal_dataset']['base_url']}/{season_url}/{league}.csv"
                pipeline = self.base_pipeline.add_operation(pd.DataFrame.assign, season=season)
                downloader = URLDataDownloader('GET', url)
                file_manager = CSVFileManager(filepath)
                validator = self._get_validator(self.config['seasonal_dataset']['validation'])
                try:
                    data = download_processor.process(
                        downloader=downloader,
                        file_manager=file_manager,
                        parser=self.parser,
                        preprocessing_pipeline=pipeline,
                        validator=validator
                    )
                except requests.exceptions.HTTPError as err:
                    if err.response.status_code in (300, 404):
                        logger.info('Data %s-%s does not exists.', league, season)
                        continue
                    raise
                except NotValidDataException:
                    logger.info('Data %s-%s is not valid.', league, season)
                    continue
                self.dataframes.append(data)
                time.sleep(2)

    def _download_new_data(self, download_processor):
        for league in self.config['new_dataset']['leagues']:
            filepath = f'data/{self.__class__.__name__}/{league}.csv'
            url = f"{self.config['new_dataset']['base_url']}/{league}.csv"
            downloader = URLDataDownloader('GET', url)
            file_manager = CSVFileManager(filepath)
            validator = self._get_validator(self.config['new_dataset']['validation'])
            try:
                data = download_processor.process(
                    downloader=downloader,
                    file_manager=file_manager,
                    parser=self.parser,
                    preprocessing_pipeline=self.base_pipeline,
                    validator=validator
                )
            except requests.exceptions.HTTPError as err:
                if err.response.status_code in (300, 404):
                    logger.info('Data %s does not exists.', league)
                    continue
                raise
            except NotValidDataException:
                logger.info('Data %s is not valid.', league)
                continue
            self.dataframes.append(data)
            time.sleep(2)

    def download_data(self, latest_date: datetime, reload: bool = False) -> pd.DataFrame:
        """
        Download data and filter it since latest_date.

        Parameters:
            latest_date (datetime): Date to filter the data on
            reload (bool): Whether to redownload all data

        Returns:
            data (pd.DataFrame): Downloaded and preprocessed valid data
        """
        
        start_date = latest_date or datetime.strptime(self.config['default_start_date'], "%Y-%m-%d").date()
        download_processor = DownloadProcessor(reload)

        self._download_seasonal_data(start_date, download_processor)
        # self._download_new_data(download_processor)

        if not self.dataframes:
            return pd.DataFrame()
        data = pd.concat(self.dataframes)
        data = data[data[self.config['database']['date_column']].dt.date > start_date]
        return data
    

# class FootballAPI(Dataset):
#     """
#     Class for managing API-Football dataset.
    
#     Attributes:
#         config: dict[str, Any]: Dataset configuration parameters
    
#     Methods:
#         download_data (latest_date): Download datasets data since latest_date
#     """

#     def __init__(self, config: dict[str, Any]) -> None:
#         """
#         Initialize class.
        
#         Parameters:
#             config (dict[str, Any]): Dataset configuration parameters

#         Returns:
#             None
#         """
#         self.config = config
#         self._downloader = APIDataDownloader(encoding='unicode_escape')
#         self._file_manager = JSONFile

#     def download_data(self, latest_date: datetime, reload: bool = False) -> pd.DataFrame:
#         """
#         Download data and filter it since latest_date.

#         Parameters:
#             latest_date (datetime): Date to filter the data on
#             reload (bool): Whether to redownload all data

#         Returns:
#             data (pd.DataFrame): Downloaded and preprocessed valid data
#         """
#         dataframes = []
#         start_date = latest_date or (datetime.today() - timedelta(days=1)).date()
#         for date in self._generate_dates(start_date, datetime.today().date()):
#             filepath = f'data/{self.__class__.__name__}/{date}/fixtures.json'
#             file = self._file_manager(filepath)
#             if file.exists() and not reload:
#                 logger.info('Reading data from file')
#                 json = file.read()
#                 preprocessed_df = self._preprocess_data(df, season)
#                 dataframes.append(preprocessed_df)
#                 continue
#             logger.info('DOWNLOADING: fixtures - %s', date)
#             params = {'date': date}
#             try:
#                 response = self._downloader.download(f"{self.config['base_url']}/fixtures", params=params)
#             except HTTPError as err:
#                 raise
#             raw_json = response.json()
#             if not self._is_valid_data(raw_json):
#                 logger.info('Data fixtures - %s is not valid.', date)
#                 continue
#             file.save(raw_json)
#             fixtures = [
#                 fixture for fixture in response['response'] 
#                 if fixture['league']['id'] in self.config['leagues']
#             ]
#             for fixture in fixtures:
#                 file_name = \
#                     f"{fixture['fixture']['id']}-"\
#                     f"{fixture['league']['name']}-"\
#                     f"{fixture['teams']['home']['name']}-"\
#                     f"{fixture['teams']['away']['name']}"
#                 filepath = f'data/{self.__class__.__name__}/{date}/fixtures/{file_name}.csv'
#                 file = self._file_manager(filepath)
#                 if file.exists() and not reload:
#                     logger.info('Reading data from file')
#                     json = file.read()
#                     preprocessed_df = self._preprocess_data(df, season)
#                     dataframes.append(preprocessed_df)
#                     continue
#                 logger.info('DOWNLOADING: fixture - %s', file_name)
#                 params = {'fixture': fixture['fixture']['id']}
#                 try:
#                     response = self._downloader.download(f"{self.config['base_url']}/fixtures/statistics", params=params)
#                 except HTTPError as err:
#                     raise
#                 raw_json = response.json()
#                 if not self._is_valid_data(raw_json):
#                     logger.info('Data fixture - %s is not valid.', file_name)
#                     continue
#                 file.save(raw_json)
#                 preprocessed_df = self._preprocess_data(raw_df, season)
#                 dataframes.append(preprocessed_df)
#                 time.sleep(2)
#         if not dataframes:
#             return pd.DataFrame()
#         data = pd.concat(dataframes)
#         data = data[data['match_date'].dt.date > start_date]
#         return data

