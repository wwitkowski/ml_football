"""Update script for Football API dataset"""

from datetime import datetime, timedelta
import json
import logging
import os
from pathlib import Path
import requests

import yaml

from database.database import Session
from etl.data_object import DataObject
from etl.data_parser import JSONDataParser
from etl.date_utils import generate_dates
from etl.download_strategy import ReplaceOnMetaFlagStrategy
from etl.files import File
from etl.process import ETL
from etl.downloader import APIDownloader
from etl.uploader import DatabaseUploader, Query


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def callback(obj):
    obj_type = obj.meta.get('object_type')
    new_objs = []
    if not obj_type == 'schedule':
        return new_objs
    obj_config = obj.meta.get('config', {})
    obj_content = json.loads(obj.file.read().decode('utf-8'))
    for match in obj_content['response']:
        if match['league']['id'] not in obj_config['leagues']:
            continue
        match_id = match['fixture']['id']
        file = File(f'{obj.file.path.resolve().parent}/{match_id}.json')
        downloader =  APIDownloader(
            'GET',
            f'{obj_config["base_url"]}/fixtures/statistics',
            params={'fixture': match_id},
            headers={
                "X-RapidAPI-Key": os.getenv("X-RapidAPI-Key"),
                "X-RapidAPI-Host": os.getenv("X-RapidAPI-Host")
            }
        )
        uploader = DatabaseUploader(
            'football_data',
            'football_api_fixture',
            on_constraint='update',
            constraint='football_api_unique'
        )
        new_obj = DataObject(
            downloader,
            file,
            uploader,
            meta={'object_type': 'fixture'}
        )
        new_objs.append(new_obj)

    return new_objs


def main() -> None:
    objects = []
    with open(Path('datasets/football_api/configuration/football_api.yaml'), 'r') as handle:
        config = yaml.safe_load(handle)

    today = datetime.today()
    start_date = today - timedelta(days=1)
    end_date = today + timedelta(days=3)

    for date in generate_dates(start_date, end_date):
        date_str = date.strftime('%Y-%m-%d')
        file = File(f'data/Football_API/{date_str}/schedule.json')
        downloader = APIDownloader(
            'GET',
            f'{config["download"]["base_url"]}/fixtures',
            params={'date': date_str},
            headers={
                "X-RapidAPI-Key": os.getenv("X-RapidAPI-Key"),
                "X-RapidAPI-Host": os.getenv("X-RapidAPI-Host")
            }
        )
        uploader = DatabaseUploader(
            'football_data',
            'football_api_schedule',
            on_constraint='update',
            constraint='football_api_unique'
        )
        obj = DataObject(
            downloader,
            file,
            uploader,
            meta={
                'type': 'schedule',
                'config': config['download']
            }
        )
        objects.append(obj)


    etl: ETL = ETL()
    download_strategy = ReplaceOnMetaFlagStrategy()
    with (
        #Session.begin() as upload_session, 
        requests.Session() as download_session
    ):
        for item in etl.process_queue(objects, strategy=download_strategy, reverse=True, limit=10):
            try:
                item_extracted = etl.extract(item, sleep_time=5, session=download_session, callback=callback)
            except requests.exceptions.HTTPError:
                continue
            # item_transformed = etl.transform(
            #     item_extracted,
            #     parser = JSONDataParser(record_path='response'),
            #     # transform_pipeline=transform_pipeline,
            # )
            # etl.load(item_transformed, upload_session)


if __name__ == '__main__':
    main()
