"""Download latest FootballCoUk data and upload to database"""

from datetime import datetime
import logging

import yaml

import sqlalchemy
from sqlalchemy import text
from pathlib import Path
from database.database import Session
from etl.dataset import Dataset, FootballDataCoUK
from etl.merging import DataMerger


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ETL:
    """
    Class responsible for running the ETL process for added datasets - Dataset classes.

    Attributes:
        data_merger (DataMerger): DataMerger class responsible for merging Datasets
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
            rewrite: bool = False,
            data_merger: DataMerger | None = None
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
        self.data_merger = data_merger

    def get_last_processed_date(self) -> datetime:
        """
        Get last run date from db. Used to run ETL on the latest data only.

        Returns:
            date (datetime): Last ETL process run date
        """
        query = 'SELECT MAX(MATCH_DATE) FROM match'
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
        last_processed_date = self.get_last_processed_date()

        data_list = []
        for dataset in self.datasets:
            data = dataset.download_data(latest_date=last_processed_date, reload=reload)
            if data.empty:
                continue
            data_list.append(data)

        if len(self.datasets) > 1 and self.data_merger is not None:
            data = self.data_merger.merge(data_list)

        if_exists = 'replace' if reload else 'append'
        logger.info('Uploading..')
        data.to_sql('match', self._session.bind, schema='data', index=False, if_exists=if_exists)
        logger.info('Done.')


def run_etl() -> None:
    """
    Configure and run etl.

    Returns: 
        None
    """
    with open(Path('etl/configuration/footballdata_co_uk.yaml'), 'r') as file:
        fd_uk_config = yaml.safe_load(file)
    fduk = FootballDataCoUK(fd_uk_config)
    with Session.begin() as session:
        etl = ETL(db_session=session)
        etl.add_dataset(fduk)
        etl.run()


if __name__ == '__main__':
    run_etl()
