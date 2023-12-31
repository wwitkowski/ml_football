"""Update script for Football Data Co UK other dataset"""

import logging
from pathlib import Path

import yaml

from database.database import Session
from etl.data_parser import CSVDataParser
from etl.process import ETL
from etl.downloader import APIDownloader
from footballdata_co_uk.pipelines import get_transform_pipeline, get_validation_pipeline

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main() -> None:
    objects = []
    with open(Path('footballdata_co_uk/configuration/footballdata_co_uk.yaml'), 'r') as file:
        config = yaml.safe_load(file)

    for league in config['new_dataset']['leagues']:
        file_path = f'data/FootballDataCoUK/{league}/{league}.csv'
        url = f"{config['new_dataset']['base_url']}/{league}.csv"
        obj = APIDownloader('GET', url, file_path, table='football_data_co_uk', schema='football_data')
        objects.append(obj)

    preprocessing_config = config['preprocessing']
    validation_config = config['new_dataset']['validation']
    transform_pipeline = get_transform_pipeline(preprocessing_config)
    validation_pipeline = get_validation_pipeline(validation_config)

    etl = ETL(sleep_time=3)
    for item in etl.extract(objects, mode='replace'):
        if item is None:
            continue
        transformed = etl.transform(
            item,
            parser = CSVDataParser(),
            transform_pipeline=transform_pipeline,
            validation_pipeline=validation_pipeline
        )
        with Session.begin() as session:
            etl.load(transformed, session, mode='append')


if __name__ == '__main__':
    main()
