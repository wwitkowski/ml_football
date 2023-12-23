"""Update script for Football Data Co UK seasonal dataset"""

from datetime import datetime
import logging
from pathlib import Path

import yaml
import pandas as pd

from database.database import Session
from etl.data_parser import CSVDataParser
from etl.data_quality import DataQualityValidator
from etl.date_utils import parse_dataframe_dates
from etl.process import ETL
from etl.downloader import APIDownloader
from etl.transform import TransformPipeline

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main():
    objects = []
    with open(Path('etl/configuration/footballdata_co_uk.yaml'), 'r') as file:
        config = yaml.safe_load(file)

    for league in config['new_dataset']['leagues']:
        file_path = f'data/FootballDataCoUK/{league}/{league}.csv'
        url = f"{config['new_dataset']['base_url']}/{league}.csv"
        obj = APIDownloader('GET', url, file_path, 'football_data_co_uk', 'data')
        objects.append(obj)

    preprocessing_config = config['preprocessing']
    transform_base_pipeline = (
        TransformPipeline()
            .add_operation(pd.DataFrame.rename, **preprocessing_config['rename'])
            .add_operation(
                lambda df: df[[col for col in df.columns if col in preprocessing_config['columns_select']]])
            .add_operation(parse_dataframe_dates, **preprocessing_config['parse_dates'])
            .add_operation(pd.DataFrame.replace, **preprocessing_config['replace'])
            .add_operation(pd.DataFrame.dropna, **preprocessing_config['dropna'])
            .add_operation(
                pd.DataFrame.apply, 
                lambda col: pd.to_numeric(col, errors='coerce') 
                if col.name in preprocessing_config['columns_to_numeric'] else col,
                axis=0
            )
            .add_operation(pd.DataFrame.convert_dtypes, **preprocessing_config['convert_dtypes']) 
    )


    validation_config = config['new_dataset']['validation']
    validation_pipeline = (
        DataQualityValidator()
            .add_condition(
                lambda df: all(col in df.columns for col in validation_config['columns_required']), 
                True
            )
    )

    etl = ETL(sleep_time=3)
    for item in etl.extract(objects, mode='replace'):
        if item is None:
            continue
        transformed = etl.transform(
            item,
            parser = CSVDataParser(),
            transform_pipeline=transform_base_pipeline,
            validation_pipeline=validation_pipeline
        )
        with Session.begin() as session:
            etl.load(transformed, session, mode='append')


if __name__ == '__main__':
    main()
