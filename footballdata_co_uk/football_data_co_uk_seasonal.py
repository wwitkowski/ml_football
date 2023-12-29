"""Update script for Football Data Co UK seasonal dataset"""

from datetime import datetime
import logging
from pathlib import Path

import yaml
import pandas as pd

from database.database import Session
from etl.data_parser import CSVDataParser
from etl.date_utils import generate_seasons
from etl.process import ETL
from etl.downloader import APIDownloader
from footballdata_co_uk.pipelines import get_transform_pipeline, get_validation_pipeline


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main() -> None:
    start_date = datetime(2000, 7, 1)
    end_date = datetime.today()
    objects = []
    with open(Path('footballdata_co_uk/configuration/footballdata_co_uk.yaml'), 'r') as file:
        config = yaml.safe_load(file)

    for season in generate_seasons(start_date, end_date):
        for league in config['seasonal_dataset']['leagues']:
            file_path = f'data/FootballDataCoUK/{season[0].replace("/","_")}/{league}.csv'
            url = f"{config['seasonal_dataset']['base_url']}/{season[0]}/{league}.csv"
            obj_meta = {'season': season[1]}
            obj = APIDownloader(
                'GET', url, file_path, table='football_data_co_uk', schema='data', meta=obj_meta)
            objects.append(obj)

    preprocessing_config = config['preprocessing']
    validation_config = config['seasonal_dataset']['validation']
    transform_base_pipeline = get_transform_pipeline(preprocessing_config)
    validation_pipeline = get_validation_pipeline(validation_config)


    etl = ETL(sleep_time=3)
    mode = 'append'
    for item in etl.extract(objects, mode=mode):
        if item is None:
            continue
        transform_pipeline = transform_base_pipeline.\
            copy().add_operation(pd.DataFrame.assign, season=item.meta['season'])
        transformed = etl.transform(
            item,
            parser = CSVDataParser(),
            transform_pipeline=transform_pipeline,
            validation_pipeline=validation_pipeline
        )

        with Session.begin() as session:
            etl.load(transformed, session, mode=mode)


if __name__ == '__main__':
    main()
