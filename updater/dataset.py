"""Datasets"""

import logging

from pathlib import Path
from urllib.parse import urlparse
from sqlalchemy import Engine
import pandas as pd

from downloader import CSVDataDownloader
from validator import PandasDatasetValidator


logger = logging.getLogger(__name__)


class Dataset:
    """
    A class to manage Dataset lifecycle from download to saving to hard drive or uploading to database

        Attributes:
            url (str): Dataset's URL
            file_path (str or Path): Dataset file path to download the data to or read from
            data (list, dict, pd.DataFrame): Dataset's data
            validated (bool): Flag indicating whether the dataset has benn successflly validated

        Methods:
            save(): Save data on hard drive
            upload(): Upload data to database
    
    """

    def __init__(self, url: str, file_path: str or Path, downloader, validator) -> None:
        """
        Initialize Dataset instance.

        Parameters:
            url (str): Dataset's URL
            file_path (str or Path): Dataset file path to download the data to or read from
            downloader: Downloader class to manage download process, has to implement download() function
            validator: Validator class to manage data validation, has to implement validate() function

        Returns:
            None
        """
        self.url = url
        self.file_path = file_path if isinstance(file_path, Path) else Path(file_path)
        self.data = None
        self.validated = False
        self._downloader = downloader
        self._validator = validator
        self._url_parsed = urlparse(self.url)

    def __repr__(self) -> str:
        class_name = type(self).__name__
        return f'{class_name}(url={self.url}, file_path={self.file_path})'
    
    def __str__(self) -> str:
        return f'{self._url_parsed.path}@{self._url_parsed.netloc}'

    def _download(self, **kwargs) -> None:
        """
        Call Downloader's download method and download data.

        Returns:
            None
        """
        self.data = self._downloader.download(self.url, **kwargs)

    def _validate(self) -> None:
        """
        Call Validator's validate method and validate data.

        Returns:
            None
        """
        logger.info('Validating dataset: %s', self)
        try:
            self.data = self._validator.validate(self.data)
            self.validated = True
        except AssertionError:
            logger.error('Dataset %s not validated.', self)

    def _read(self) -> None:
        """
        Read data from file.

        Returns:
            None
        """
        logger.info('Reading from file %s: %s', self, self.file_path)
        try:
            self.data = pd.read_csv(self.file_path)
        except Exception:
            logger.exception('Error when loading the file: ')

    def save(self) -> None:
        """
        Save dataset to hard drive.

        Returns:
            None
        """
        self.file_path.parent.mkdir(parents=True, exist_ok=True)
        logger.info('Saving dataset: %s, path: %s', self, self.file_path)
        if isinstance(self.data, pd.DataFrame):
            self.data.to_csv(self.file_path, index=False)
        else:
             raise NotImplementedError('Saving data only implemented for pandas DataFrame objects')   

    def upload(self, conn: Engine, table: str, **kwargs) -> None:
        """
        Upload data to database.

        Parameters:
            conn: database connection

        Returns:
            None
        """
        if isinstance(self.data, pd.DataFrame):
            logger.info('Uploading dataset: %s', self)
            self.data.to_sql(table, conn, **kwargs)
            logger.info('Uploading succesful')
        else:
            raise NotImplementedError('Uploading data only implemented for pandas DataFrame objects')   


class CSVDataset(Dataset):
    """
    A class for managing csv dataset.

    Attributes:
        data (list, dict, pd.DataFrame): Dataset's data
        validation_config (Optional[dict]): Validation configuration parameters

    Methods:
        parse_dates(col, date_formats): Convert a pandas DataFrame column to datetime

    """

    def __init__(self, url: str, file_path: str or Path, validation_config: dict = None) -> None:
        """
        Initialize CSVDataset class

        Parameters:
            url (str): Dataset's URL
            file_path (str or Path): Dataset file path to download the data to or read from
            validation_config (dict): Data validation configuration
        
        Retruns:
            None
        """
        downloader = CSVDataDownloader()
        validator = PandasDatasetValidator(validation_config) if validation_config else None
        super().__init__(url, file_path, downloader, validator)

    def parse_dates(self, col: str, date_formats: list[str]) -> None:
        """
        Convert a pandas DataFrame column to datetime using provided string formats.

        Raise ValueError exception when none of provided date_formats matches the date string.

        Parameters:
            col (str): Name of the column to be converted
            date_formats (list[str]): List of the formats to try to match to the dates in column

        Returns:
            None
        """
        for date_format in date_formats:
            try:
                self.data[col] = pd.to_datetime(self.data[col], format=date_format)
                return
            except ValueError:
                pass
        raise ValueError(f'None of {date_formats} match {col} date format')
    
    def load(self) -> None:
        """
        Load data from internet or hard drive.

        Returns:
            None
        """
        if not self.file_path.is_file():
            logger.info('Downloading dataset: %s, URL: %s', self, self.url)
            self._download()
            if self._validator is not None:
                self._validate()
        else:
            self._read()

        

