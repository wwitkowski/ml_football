from abc import ABC, abstractmethod
import logging
from typing import Any, Dict
import requests


logger = logging.getLogger(__name__)


class Downloader(ABC):
    """
    Abstract base class defining a downloader interface.

    Attributes:
        file_path (str): The path to store the downloaded file.
        table (str | None): The db table name (optional, can be None if data won't be loaded into db).
        schema (str | None): The db schema name (optional, can be None if data won't be loaded into db).
        meta (Dict | None): Metadata used for storing additional info about the object.

    """
    def __init__(self, file_path: str, table: str | None, schema: str | None, meta: Dict | None) -> None:
        self.file_path = file_path
        self.table = table
        self.schema = schema
        self.meta = meta or {}

    @abstractmethod
    def download(self, session: Any | None = None) -> Any:
        """
        Abstract method to download data.

        Parameters:
            session (Any | None): Optional session to use for the download.

        Returns:
            Any: Content retrieved from the download.
        """
        pass


class APIDownloader(Downloader):
    """
    Implementation of a downloader for API endpoints.

    Attributes:
        method (str): The HTTP method used for the API request.
        url (str): The URL for the API endpoint.
        file_path (str): The path to store the downloaded file.
        table (str | None): The table name (optional, can be None if not applicable).
        schema (str | None): The schema name (optional, can be None if not applicable).
        meta (Dict | None): Metadata used for storing additional info about the object.
        download_kwargs (dict): Additional keyword arguments for the download.
    """
    def __init__(
            self, 
            method: str, 
            url: str, 
            file_path: str, 
            table: str | None = None, 
            schema: str | None = None, 
            meta: Dict | None = None,
            **download_kwargs: dict
        ) -> None:
        super().__init__(file_path, table=table, schema=schema, meta=meta)
        self.method = method
        self.url = url
        self.download_kwargs = download_kwargs

    
    def __repr__(self) -> str:
        """
        Returns a representation of the object.

        Returns:
            str: Representation of the object.
        """
        return f'APIDownloader(file_path={self.file_path}, method={self.method}, url={self.url}, db={self.schema}/{self.table})'

    def __str__(self) -> str:
        """
        Returns a string representation of the object.

        Returns:
            str: String representation of the object.
        """
        return f'APIDownloader {self.url}@{self.file_path}'
    
    def download(self, session: requests.Session | None = None) -> bytes:
        """
        Download data from the specified URL using the provided method and options.

        Parameters:
            session (requests.Session | None): Optional requests session to use for the download.

        Returns:
            bytes: Content retrieved from the download.

        Raises:
            requests.HTTPError: If the response status code is not a success code.
        """
        logger.info('DOWNLOADING: %s', self.url)
        session = session or requests.Session()
        response = session.request(self.method, self.url, **self.download_kwargs)
        response.raise_for_status()
        return response.content
