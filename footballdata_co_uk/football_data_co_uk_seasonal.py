"""Update script for Football Data Co UK seasonal dataset"""

from datetime import datetime
import logging
from pathlib import Path

import yaml
import pandas as pd

from database.database import Session
from etl.data_parser import CSVDataParser
from etl.date_utils import generate_seasons
from etl.download_strategy import ReplaceOnMetaFlagStrategy
from etl.process import ETL
from etl.downloader import APIDownloader
from footballdata_co_uk.pipelines import get_transform_pipeline, get_validation_pipeline


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
BACKTRACK = 2


def main() -> None:
    start_date = datetime(2000, 7, 1)
    end_date = datetime.today()
    objects = []
    with open(Path('footballdata_co_uk/configuration/footballdata_co_uk.yaml'), 'r') as file:
        config = yaml.safe_load(file)

    seasons = list(generate_seasons(start_date, end_date))
    num_seasons = len(seasons)
    for season in seasons:
        replace = True if season in seasons[num_seasons-BACKTRACK:] else False
        for league in config['seasonal_dataset']['leagues']:
            file_path = f'data/FootballDataCoUK/{season[0].replace("/","_")}/{league}.csv'
            url = f"{config['seasonal_dataset']['base_url']}/{season[0]}/{league}.csv"
            obj_meta = {'season': season[1], 'replace': replace}
            obj = APIDownloader(
                'GET', url, file_path, table='football_data_co_uk', schema='football_data', meta=obj_meta)
            objects.append(obj)

    preprocessing_config = config['preprocessing']
    validation_config = config['seasonal_dataset']['validation']
    transform_base_pipeline = get_transform_pipeline(preprocessing_config)
    validation_pipeline = get_validation_pipeline(validation_config)


    etl = ETL(sleep_time=3)
    download_strategy = ReplaceOnMetaFlagStrategy()
    with Session.begin() as dsession:
        for item in etl.extract(objects, strategy=download_strategy):
            if item is None:
                continue
            transform_pipeline = transform_base_pipeline.\
                copy().add_operation(pd.DataFrame.assign, season=item.meta['season'])
            transformed = etl.transform(
                item,
                parser = CSVDataParser(encoding='unicode_escape'),
                transform_pipeline=transform_pipeline,
                validation_pipeline=validation_pipeline
            )
            etl.load(transformed, dsession, mode='append')


if __name__ == '__main__':
    main()
