"""Download ETL Processor"""
import time
import logging
from typing import Any, Callable, Iterator, List, Tuple, Type, TypeVar

import pandas as pd
import requests
from sqlalchemy import text

from etl.data_parser import DataParser
from etl.data_quality import DataQualityValidator
from etl.download_strategy import AppendStrategy, DownloadStrategy
from etl.downloader import Downloader, DownloaderObject
from etl.files import File
from etl.transform import TransformPipeline


logger = logging.getLogger(__name__)


class ETL:
    """
    ETL processor class performing data extraction, transformation, and loading.

    Attributes:
        sleep_time (int): Time to sleep between extraction cycles
        file_handler (File): File handling class instance
    """

    def __init__(self, sleep_time: int = 0) -> None:
        """
        Initialize ETL class.

        Parameters:
            sleep_time (int, optional): Time to sleep between extraction cycles
            file_handler (File, optional): File handling class instance

        Returns:
            None
        """
        self._queue = []
        self.sleep_time = sleep_time

    def process_queue(
            self, 
            queue, 
            strategy: DownloadStrategy = AppendStrategy(), 
            reverse=False
        ) -> Iterator[DownloaderObject]:
        self._queue = queue
        while self._queue:
            queue_obj = self._queue.pop(-int(reverse))
            if strategy.is_download_required(queue_obj):
                yield queue_obj

    def extract(
            self,
            obj: DownloaderObject,
            session: Any | None = None,
            callback: Callable | None = None
        ) -> Iterator[DownloaderObject]:
        """
        Extract data from a queue of downloaders.

        Parameters:
            obj (DownloaderObject): Downloader object
            session (Any | None): Extract session
            callback (Callable | None): Callback function for generating new download objects

        Yields:
            DownloaderObject: Object with successfully extracted data
        """
        content = obj.download(session)
        obj.file.save(content)
        time.sleep(self.sleep_time)
        if callback:
            new_objects = callback(obj.file.read())
            self._queue.extend(new_objects)
        return obj


    def transform(
            self,
            obj: DownloaderObject,
            parser: DataParser | None = None,
            transform_pipeline: TransformPipeline | None = None,
            validation_pipeline: DataQualityValidator | None = None
        ) -> Tuple[DownloaderObject, Any]:
        """
        Transform the data using specified pipelines.

        Parameters:
            obj (DownloaderObject): Downloader object
            parser (DataParser | None): Parser object
            transform_pipeline (TransformPipeline | None): Transform pipeline
            validation_pipeline (DataQualityValidator | None): Validation pipeline

        Returns:
            Tuple[DownloaderObject, Any]: Tuple containing the object and transformed data
        """
        data = obj.file.read()
        if parser:
            data = parser.parse(data)
        if validation_pipeline:
            validation_pipeline.validate(data)
        if transform_pipeline:
            data = transform_pipeline.apply(data)
        return (obj, data)

    def load(
            self,
            dataset: Tuple[DownloaderObject, pd.DataFrame],
            session: Any,
            mode: str,
        ) -> None:
        """
        Load data into the database.

        Parameters:
            dataset (Tuple[DownloaderObject, pd.DataFrame]): Tuple containing the object and DataFrame
            session (Any): Database session
            mode (str): Load mode

        Returns:
            None
        """
        obj, data = dataset
        logger.info('UPLOADING: %s to %s.%s', obj, obj.schema, obj.table)
        if mode == 'replace':
            placeholders = ', '.join([':' + col for col in data.columns])
            columns = ', '.join(data.columns)
            query = text(
                f"INSERT INTO {obj.schema}.{obj.table} ({columns}) VALUES ({placeholders}) "
                f"ON CONFLICT DO UPDATE SET {', '.join(f'{col} = EXCLUDED.{col}' for col in data.columns)}"
            )
            session.execute(query, [dict(row) for row in data.to_dict(orient='records')])
        elif mode == 'append':
            placeholders = ', '.join([':' + col for col in data.columns])
            columns = ', '.join(data.columns)
            query = text(
                f"INSERT INTO {obj.schema}.{obj.table} ({columns}) VALUES ({placeholders}) "
                f"ON CONFLICT DO NOTHING"
            )
            session.execute(query, [dict(row) for row in data.to_dict(orient='records')])
