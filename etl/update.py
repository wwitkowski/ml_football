"""Download latest FootballCoUk data and upload to database"""

from datetime import datetime
import logging

import yaml
import pandas as pd

import sqlalchemy
from etl.dataset import Dataset, FootballDataCoUK
from database.database import Session
from etl.merging import DataMerger

DEFAULT_START_DATE = '1900-01-01'
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ETL:
    """
    Class responsible for running the ETL process for added datasets - Dataset classes.

    Attributes:
        default_start_date (str): Default date filter datasets on
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
            db_connection: sqlalchemy.orm.session.Session, 
            default_start_date: str = DEFAULT_START_DATE, 
            data_merger: DataMerger | None = None
        ):
        """
        Initialize ETL class

        Parameters:
            db_connection (sqlalchemy.orm.session.Session): Connection to the database
            default_start_date (str): Default date filter datasets on
        
        Returns:
            None
        """
        self._connection = db_connection
        self.default_start_date = default_start_date
        self.data_merger = data_merger

    def get_last_processed_date(self) -> datetime:
        """
        Get last run date from db. Used to run ETL on the latest data only.

        Returns:
            date (datetime): Last ETL process run date
        """
        return datetime.strptime(self.default_start_date, '%Y-%m-%d').date()
    
    def add_dataset(self, dataset: Dataset) -> None:
        """
        Add dataset to ETL process run

        Parameters:
            dataset (Dataset): Dataset to be added

        Returns:
            None
        """
        self.datasets.append(dataset)

    def run(self) -> None:
        """
        Run the ETL process

        Returns:
            None
        """
        last_processed_date = self.get_last_processed_date()

        data_list = []
        for dataset in self.datasets:
            data = dataset.download_data(latest_date=last_processed_date)
            if data.empty:
                continue
            data_list.append(data)

        if len(self.datasets) > 1 and self.data_merger is not None:
            merged_data = self.data_merger.merge(data_list)
            pd.to_sql(merged_data, self.connection)


def run_etl() -> None:
    """
    Configure and run etl.

    Returns: 
        None
    """
    with open('updater\\configuration\\footballdata_co_uk.yaml', 'r') as file:
        fd_uk_config = yaml.safe_load(file)
    fduk = FootballDataCoUK(fd_uk_config)
    with Session.begin() as session:
        etl = ETL(db_connection=session)
        etl.add_dataset(fduk)
        etl.run()


if __name__ == '__main__':
    run_etl()
