"""Download ETL Processor"""
import time
import logging
from typing import Any, Callable, Iterator, List, Tuple

from etl.data_object import DataObject
from etl.data_parser import DataParser
from etl.data_quality import DataQualityValidator
from etl.download_strategy import AppendStrategy, DownloadStrategy
from etl.transform import TransformPipeline

logger = logging.getLogger(__name__)


# TODO: add logging
class ETL:
    """
    ETL processor class performing data extraction, transformation, and loading.
    """
    def __init__(self, strategy: DownloadStrategy | None = None):
        self.strategy = strategy or AppendStrategy()
        self._queue = []

    def process_queue(
        self,
        items: List[DataObject],
        reverse: bool = False,
        limit: int | None = None
    ) -> Iterator[DataObject]:
        """
        Process the queue of objects.

        Args:
            items (List[DataObject]): List of objects to process
            reverse (bool): Flag to reverse the queue (default: False)
            limit (int): Limit number of objects to be processed (useful when source has a requests limit)

        Yields:
            Iterator[DataObject]: Iterator over downloaded objects
        """
        self._queue.extend(items)
        obj_counter = 0
        while self._queue:
            queue_obj = self._queue.pop(-int(reverse))
            if limit is not None and obj_counter >= limit:
                break
            yield queue_obj
            obj_counter += 1

    def extract(
        self,
        obj: DataObject,
        sleep_time: int = 0,
        session: Any | None = None,
        callback: Callable | None = None
    ) -> DataObject:
        """
        Extract data and save it.

        Args:
            obj (DownloaderObject): Downloader instance to extract data from.
            sleep_time (int): Sleep time between each requests in seconds.
            session (Any | None): Extract session.
            callback (Callable | None): Callback function for generating new download objects.

        Returns:
            Downloader: Object that had its data downlaoded.
        """
        if self.strategy.is_download_required(obj):
            content = obj.downloader.download(session)
            obj.file.save(content)
            time.sleep(sleep_time)
        if callback:
            new_objects = callback(obj)
            self._queue.extend(new_objects)
        return obj

    def transform(
        self,
        obj: DataObject,
        parser: DataParser | None = None,
        transform_pipeline: TransformPipeline | None = None,
        validation_pipeline: DataQualityValidator | None = None
    ) -> Tuple[DataObject, Any]:
        """
        Parse data adn then transform the it using specified pipelines.

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
        obj: DataObject,
        data: Any,
        session: Any
    ) -> None:
        """
        Load data into the database .

        Args:
            obj (DataObject): Processed data object with uploader attribute.
            data (Any): Data to be uploaded.
            session (Any): Upload session.

        Returns:
            None
        """
        obj.uploader.upload(session, data)
