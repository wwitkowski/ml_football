"""Download ETL Processor"""
import time
import logging
from typing import Any, Callable, Generic, Iterator, List, Tuple, TypeVar
import pandas as pd
from sqlalchemy import text

from etl.data_parser import DataParser
from etl.data_quality import DataQualityValidator
from etl.download_strategy import AppendStrategy, DownloadStrategy
from etl.downloader import Downloader
from etl.transform import TransformPipeline

logger = logging.getLogger(__name__)
DownloaderObject = TypeVar('DownloaderObject', bound=Downloader)

class ETL(Generic[DownloaderObject]):
    """
    ETL processor class performing data extraction, transformation, and loading.

    Attributes:
        sleep_time (int): Time to sleep between extraction cycles
        file_handler (File): File handling class instance
    """

    def __init__(self, sleep_time: int = 0) -> None:
        """
        Initialize ETL class.

        Args:
            sleep_time (int | None): Time to sleep between extraction cycles

        Returns:
            None
        """
        self._queue: List[DownloaderObject] = []
        self.sleep_time = sleep_time

    def process_queue(
        self,
        queue: List[DownloaderObject],
        strategy: DownloadStrategy = AppendStrategy(),
        reverse: bool = False
    ) -> Iterator[DownloaderObject]:
        """
        Process the queue of objects based on a download strategy.

        Args:
            queue (List[DownloaderObject]): List of Downloader instances
            strategy (DownloadStrategy): Download strategy instance (default: AppendStrategy())
            reverse (bool): Flag to reverse the queue (default: False)

        Yields:
            Iterator[DownloaderObject]: Iterator over downloaded objects
        """
        self._queue = queue
        while self._queue:
            queue_obj = self._queue.pop(-int(reverse))
            if strategy and strategy.is_download_required(queue_obj):
                yield queue_obj

    def extract(
        self,
        obj: DownloaderObject,
        session: Any | None = None,
        callback: Callable | None = None
    ) -> DownloaderObject:
        """
        Extract data from a Downloader and save it.

        Args:
            obj (DownloaderObject): Downloader instance to extract data from
            session (Any | None): Extract session
            callback (Callable | None): Callback function for generating new download objects

        Returns:
            Downloader: Object taht had its data downlaoded.
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

        Args:
            obj (DownloaderObject): Downloader instance to transform data from
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
        return obj, data

    def load(
        self,
        dataset: Tuple[DownloaderObject, pd.DataFrame],
        session: Any,
        mode: str = 'replace'
    ) -> None:
        """
        Load data into the database.

        Args:
            dataset (Tuple[Downloader, pd.DataFrame]): Tuple containing the object and DataFrame
            session (Any): Database session
            mode (str): Load mode (default: 'replace')

        Returns:
            None
        """
        obj, data = dataset
        logger.info('UPLOADING: %s to %s.%s', obj, obj.schema, obj.table)
        placeholders = ', '.join([':' + col for col in data.columns])
        columns = ', '.join(data.columns)
        if mode == 'replace':
            query = text(
                f"INSERT INTO {obj.schema}.{obj.table} ({columns}) VALUES ({placeholders}) "
                f"ON CONFLICT ON CONSTRAINT {obj.table}_unique DO UPDATE SET "
                f"{', '.join(f'{col} = EXCLUDED.{col}' for col in data.columns)}"
            )
        elif mode == 'append':
            query = text(
                f"INSERT INTO {obj.schema}.{obj.table} ({columns}) VALUES ({placeholders}) "
                f"ON CONFLICT ON CONSTRAINT {obj.table}_unique DO NOTHING"
            )
        session.execute(query, [dict(row) for row in data.to_dict(orient='records')])
