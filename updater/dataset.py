import logging

import pandas as pd
from pathlib import Path
from urllib.parse import urlparse
from urllib.error import HTTPError

from downloader import CSVDataDownloader


logger = logging.getLogger(__name__)


class Dataset:

    def __init__(self, url, file_path, downloader=CSVDataDownloader, validator=None, validation_config=None):
        self.url = url
        self.url_parsed = urlparse(self.url)
        self.file_path = Path(file_path)
        self.downloader = downloader()
        self.validator = validator(validation_config) if validator is not None else None
        self.validated = False
        self.data = None

    def __repr__(self):
        class_name = type(self).__name__
        return f'{class_name}(url={self.url}, file_path={self.file_path})'
    
    def __str__(self):
        return f'{self.url_parsed.path}@{self.url_parsed.netloc}'

    def _download(self):
        self.data = self.downloader.download(self.url, encoding='unicode_escape')

    def _validate(self):
        self.data = self.validator.validate(self.data)

    def _save(self):
        self.file_path.parent.mkdir(parents=True, exist_ok=True)
        self.data.to_csv(self.file_path, index=False)

    def parse_dates(self, col, date_formats):
        for date_format in date_formats:
            try:
                self.data[col] = pd.to_datetime(self.data[col], format=date_format)
                return
            except ValueError:
                pass
        raise ValueError(f'None of {date_formats} match {col} date format')
    
    def load(self, overwrite=False):
        if not self.file_path.is_file() or overwrite==True:
            logger.info('Downloading dataset: %s, URL: %s', self, self.url)
            try:
                self._download()
            except HTTPError as e:
                if e.code in (300, 404):
                    logger.warning('Dataset %s does not exist', self)
                    return
                else:
                    raise
            if self.validator is not None:
                logger.info('Validating dataset: %s', self)
                try:
                    self._validate()
                    self.validated = True
                except AssertionError:
                    logger.error('Dataset %s not validated.', self)
                    return
            logger.info('Saving dataset: %s, path: %s', self, self.file_path)
            self._save()
            return
        logger.info('Reading from file %s: %s', self, self.file_path)
        self.data = pd.read_csv(self.file_path)

    def upload(self, conn, table, **kwargs):
        logger.info('Uploading dataset: %s', self)
        self.data.to_sql(table, conn, **kwargs)
        logger.info('Uploading succesful')