import sys
import time
import logging
from datetime import datetime
from collections import defaultdict

import requests
from urllib.error import HTTPError
import pandas as pd
from sqlalchemy.exc import IntegrityError
from pathlib import Path
from const import FOOTBALL_CO_UK_CONFIG

file = Path(__file__).resolve()
parent, root = file.parent, file.parents[1]
sys.path.append(str(root))

try:
    sys.path.remove(str(parent))
except ValueError: # Already removed
    pass

from utils.postgres import PGDatabase

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def season_range(start_date, end_date):
    for n in range(end_date.year - start_date.year + 1):
        year = start_date.replace(year=start_date.year + n).strftime('%y')
        year_plus_one = start_date.replace(year=start_date.year + n + 1).strftime('%y')
        yield f'{year}{year_plus_one}'


class CSVDataDownloader:

    @staticmethod
    def _is_empty_line(line):
        if not line:
            return True
        if not any([value for value in line]):
            return True
        return False

    @staticmethod
    def _parse_byte_line(line, encoding):
        return line.decode(encoding).split(',')

    def _download_bad_csv_lines(self, url, encoding):
        r = requests.get(url)
        r.raise_for_status()
        content = r.iter_lines()
        header = self._parse_byte_line(next(content), encoding)
        lines = [
            parsed_line[:len(header)] for line in content 
            if not self._is_empty_line(parsed_line := self._parse_byte_line(line, encoding))
        ]
        data = pd.DataFrame(data=lines, columns=header)
        return data

    def download(self, url, encoding='utf8', **kwargs):
        try:
            data = pd.read_csv(url, encoding=encoding, **kwargs)
        except pd.errors.ParserError:
            data = self._download_bad_csv_lines(url, encoding)
        return data
    

class DatasetValidator:

    def __init__(self, config):
        self.config = config

    def _validate_columns(self, data):
        assert all([col in data.columns for col in self.config['columns']['valid_columns']])

    def _validate_rows(self, data):
        if threshold:=self.config['rows'].get('thresholds'):
            data = data.dropna(thresh=int(threshold*len(data.columns)))
        data = data.dropna(subset=self.config['columns']['valid_columns'])
        return data

    def _validate_dtypes(self, data):
        for col, dtype in self.config['valid_dtypes'].items():
            if col not in data.columns:
                continue
            if dtype in ('int64', 'float64'):
                data.loc[:, col] = pd.to_numeric(data[col], errors='coerce')
        return data
    
    def validate(self, data):
        self._validate_columns(data)
        data = self._validate_rows(data)
        data = self._validate_dtypes(data)
        return data


class FootballDataCoUkDataset:
    base_url = 'https://www.football-data.co.uk/mmz4281/'

    def __init__(self, league, season, validation_config, file_path=None):
        self.league = league
        self.season = season
        self.file_path = Path(file_path) if file_path is not None else Path(f'data/{self.league}/{season}.csv')
        self.url = f'{self.base_url}{season}/{league}.csv'
        self.downloader = CSVDataDownloader()
        self.validator = DatasetValidator(validation_config)
        self.validated = False
        self.data = None

    def __repr__(self):
        class_name = type(self).__name__
        return f'{class_name}(League={self.league}, Season={self.season})'
    
    def __str__(self):
        return f'{self.league} - {self.season}'

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


def update():
    end_date = datetime.today().date()
    pg = PGDatabase()
    uploaded_seasons = pg.fetch('SELECT DISTINCT season, league FROM match')
    season_dict = defaultdict(list)
    for season, league in uploaded_seasons:
        season_dict[league].append(season)
    for league in FOOTBALL_CO_UK_CONFIG['leagues']:
        for season in season_range(FOOTBALL_CO_UK_CONFIG['start_date'], end_date):
            if season in season_dict[league] and season != max(season_dict[league]):
                continue
            dataset = FootballDataCoUkDataset(
                league, 
                season, 
                validation_config=FOOTBALL_CO_UK_CONFIG['validation']
            )
            dataset.load(overwrite=True)
            if dataset.data is None or not dataset.validated:
                continue
            dataset.data = dataset.data[
                [col for col in dataset.data.columns if col in FOOTBALL_CO_UK_CONFIG['columns_select']]
            ]
            dataset.parse_dates('Date', date_formats=['%d/%m/%y', '%d/%m/%Y'])
            dataset.data.rename(columns=FOOTBALL_CO_UK_CONFIG['column_names'], inplace=True)
            dataset.data = dataset.data.assign(season=season)
            pg.execute(f"DELETE FROM match WHERE league = '{league}' AND season = '{season}'")
            dataset.upload(pg.engine, 'match', if_exists='append', index=False)
            time.sleep(1)


if __name__ == '__main__':
    update()
