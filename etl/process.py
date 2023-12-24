import time
import logging
from typing import Any, Callable, Iterator, List, Tuple, Type

import pandas as pd
import requests
from sqlalchemy import text
from etl.data_parser import DataParser
from etl.data_quality import DataQualityValidator

from etl.exceptions import InvalidDataException
from etl.downloader import Downloader
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

    def __init__(self, sleep_time: int = 0, file_handler: Type[File] = File) -> None:
        """
        Initialize ETL class.

        Parameters:
            sleep_time (int, optional): Time to sleep between extraction cycles
            file_handler (File, optional): File handling class instance

        Returns:
            None
        """
        self.sleep_time = sleep_time
        self.file_handler = file_handler
    
    def extract(
            self, 
            queue: List[Downloader],
            mode: str,
            session: Any | None = None, 
            callback: Callable | None = None
        ) -> Iterator[Downloader]:
        """
        Extract data from a queue of downloaders.

        Parameters:
            queue (List[Downloader]): Queue of downloaders
            mode (str): Extraction mode
            session (Any | None): Extract session
            callback (Callable | None): Callback function for generating new download objects

        Yields:
            Downloader: Object with successfully extracted data
        """
        while queue:
            obj: Downloader = queue.pop()
            file = self.file_handler(obj.file_path)
            if not file.exists() or mode == 'replace':
                try:
                    content = obj.download(session)
                except requests.exceptions.HTTPError as err:
                    logger.warning('Error %d for %s', err.response.status_code, obj)
                    time.sleep(self.sleep_time)
                    continue
                file.save(content)
            if callback:
                new_objects = callback(content)
                queue.extend(new_objects)
            time.sleep(self.sleep_time)
            yield obj

    def transform(
            self, 
            obj: Downloader, 
            parser: DataParser | None = None, 
            transform_pipeline: TransformPipeline | None = None, 
            validation_pipeline: DataQualityValidator | None = None
        ) -> Tuple[Downloader, Any]:
        """
        Transform the data using specified pipelines.

        Parameters:
            obj (Downloader): Downloader object
            parser (DataParser | None): Parser object
            transform_pipeline (TransformPipeline | None): Transform pipeline
            validation_pipeline (DataQualityValidator | None): Validation pipeline

        Returns:
            Tuple[Downloader, Any]: Tuple containing the object and transformed data
        """
        file = self.file_handler(obj.file_path)
        data = file.read()
        if parser:  
            data = parser.parse(data)
        if validation_pipeline:
            validation_pipeline.validate(data)
        if transform_pipeline:
            data = transform_pipeline.apply(data)
        return (obj, data)

    def load(
            self, 
            dataset: Tuple[Downloader, pd.DataFrame],
            session: Any,
            mode: str,
        ) -> None:
        """
        Load data into the database.

        Parameters:
            dataset (Tuple[Downloader, pd.DataFrame]): Tuple containing the object and DataFrame
            session (Any): Database session
            mode (str): Load mode

        Returns:
            None
        """
        obj, data = dataset
        if mode == 'replace':
            session.execute(f"DELETE FROM {obj.schema}.{obj.table}")
            data.to_sql(obj.table, session.bind, schema=obj.schema, if_exists='append')
        elif mode == 'append':
            placeholders = ', '.join([':' + col for col in data.columns])
            columns = ', '.join(data.columns)
            query = text(
                f"INSERT INTO {obj.schema}.{obj.table} ({columns}) VALUES ({placeholders}) "
                f"ON CONFLICT DO UPDATE SET {', '.join(f'{col} = EXCLUDED.{col}' for col in data.columns)}"
            )
            session.execute(query, [dict(row) for row in data.to_dict(orient='records')])
