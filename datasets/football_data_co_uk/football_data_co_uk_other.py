"""Update script for Football Data Co UK other dataset"""

import logging
from pathlib import Path
import requests

import yaml

from database.database import Session
from etl.data_parser import CSVDataParser
from etl.download_strategy import ReplaceStrategy
from etl.files import File
from etl.process import ETL
from etl.downloader import APIDownloader
from datasets.football_data_co_uk.pipelines import get_transform_pipeline, get_validation_pipeline

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main() -> None:
    objects = []
    with open(Path('datasets/configuration/footballdata_co_uk.yaml'), 'r') as handle:
        config = yaml.safe_load(handle)

    for league in config['new_dataset']['leagues']:
        file = File(f'data/FootballDataCoUK/{league}/{league}.csv')
        url = f"{config['new_dataset']['base_url']}/{league}.csv"
        obj = APIDownloader('GET', url, file, table='football_data_co_uk', schema='football_data')
        objects.append(obj)

    preprocessing_config = config['preprocessing']
    validation_config = config['new_dataset']['validation']
    transform_pipeline = get_transform_pipeline(preprocessing_config)
    validation_pipeline = get_validation_pipeline(validation_config)

    etl: ETL = ETL(sleep_time=3)
    download_strategy = ReplaceStrategy()
    with Session.begin() as upload_session, requests.Session() as download_session:
        for item in etl.process_queue(objects, strategy=download_strategy):
            try:
                item_extracted = etl.extract(item, session=download_session)
            except requests.exceptions.HTTPError:
                continue
            item_transformed = etl.transform(
                item_extracted,
                parser = CSVDataParser(encoding='unicode_escape'),
                transform_pipeline=transform_pipeline,
                validation_pipeline=validation_pipeline
            )
            etl.load(item_transformed, upload_session)


if __name__ == '__main__':
    main()
