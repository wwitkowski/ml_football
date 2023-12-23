"""Download latest FootballCoUk data and upload to database"""

from datetime import datetime
import logging
import os

import yaml

import sqlalchemy
from sqlalchemy import text
from pathlib import Path
from database.database import Session
from etl.data_parser import CSVDataParser
from etl.data_quality import DataQualityValidator
from etl.dataset import Dataset, FootballDataCoUK
from etl.date_utils import parse_dates
from etl.process import DownloadProcessor
from etl.transform import PreprocessingPipeline


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ETL:
    """
    Class responsible for running the ETL process for added datasets - Dataset classes.

    Attributes:
        datasets (list[Dataset]): List of Datasets

    Methods:
        get_last_processed_date(): Get last run date from db. Used to run ETL on the latest data only.
        add_dataset(dataset): Add to Dataset to ETL process
        run(): Run the ETL process
    """
    datasets = []

    def __init__(
            self,
            db_session: sqlalchemy.orm.session.Session,
            rewrite: bool = False
        ):
        """
        Initialize ETL class

        Parameters:
            db_session (sqlalchemy.orm.session.Session): Database session
            rewrite (bool): Whether to redownload and rewrite the data
        
        Returns:
            None
        """
        self._session = db_session
        self._rewrite = rewrite

    def get_last_processed_date(self, column: str, table: str) -> datetime:
        """
        Get last run date from db table. Used to run ETL on the latest data only.

        Parameters:
            columns (str): columns with the date.
            table (str): DB table name.

        Returns:
            date (datetime): Last ETL process run date
        """
        query = f'SELECT MAX({column}) FROM {table}'
        return self._session.execute(text(query)).fetchone()[0]

    def add_dataset(self, dataset: Dataset) -> None:
        """
        Add dataset to ETL process run

        Parameters:
            dataset (Dataset): Dataset to be added

        Returns:
            None
        """
        self.datasets.append(dataset)

    def run(self, reload: bool = False) -> None:
        """
        Run the ETL process

        Parameters:
            reload (bool): redownload all data

        Returns:
            None
        """

        for dataset in self.datasets:
            sql_date_column = dataset.config['database']['date_column']
            sql_table_name = dataset.config['database']['table_name']
            last_processed_date = self.get_last_processed_date(sql_date_column, sql_table_name)

            data = dataset.download_data(latest_date=last_processed_date, reload=reload)
            if data.empty:
                continue

            if_exists = 'replace' if reload else 'append'
            logger.info('Uploading..')
            data.to_sql(sql_table_name, self._session.bind, schema='data', index=False, if_exists=if_exists)
            logger.info('Done.')


def run_etl() -> None:
    """
    Configure and run etl.

    Returns: 
        None
    """
    with open(Path('etl/configuration/footballdata_co_uk.yaml'), 'r') as file:
        fd_config = yaml.safe_load(file)
    fd_dataset = FootballDataCoUK(fd_config)
    with Session.begin() as session:
        etl = ETL(db_session=session)
        etl.add_dataset(fd_dataset)
        etl.run()


if __name__ == '__main__':
    run_etl()

    import requests
    selected_league_ids = [
        39, 40, 41, # 42 England
        135, 136,  # Italy
        78, 79, # Germany
        61, # 62 France
        140, 141, # Spain
        88, # Netherlands
        203, # Turkey
        94, # Portugal
        144, # Belgium
        119, # Denmark
        106, # Poland
        71, # Brazil
        253, # USA
        128, # Argentina
        103, # Norway
        113, # Sweden
    ]
    url = "https://api-football-v1.p.rapidapi.com/v3/fixtures"

    querystring = {"date":"2023-10-07"}

    headers = {
        "X-RapidAPI-Key": os.getenv("X-RapidAPI-Key"),
        "X-RapidAPI-Host": os.getenv("X-RapidAPI-Host"),
    }

    response = requests.get(url, headers=headers, params=querystring).json()

    matches = [match for match in response['response'] if match['league']['id'] in selected_league_ids]

    print()
