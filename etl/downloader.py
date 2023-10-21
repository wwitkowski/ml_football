"""Downloaders"""

from abc import ABC, abstractmethod
import logging
from typing import Callable, ParamSpec, Any

import requests


logger = logging.getLogger(__name__)


class DataDownloader(ABC):
    """Abstract DataDownloader class"""

    @abstractmethod
    def download(self) -> Any: # pragma: no cover
        """abstract download function"""


class URLDataDownloader:
    """
    Class for managing data download from internet websites.

    Methods:
        download(url, encoding, **kwargs): Download csv data
    """

    def __init__(
            self, 
            method: str, 
            url: str,
            session: requests.Session | None = None, 
            **kwargs
        ) -> None:
        """
        Initialize class.

        Parameters:
            session (requests.Session): Requests Session

        Returns:
            None
        """
        self._session = session or requests.Session()
        self.method = method
        self.url = url
        self.requests_kwargs = kwargs


    def download(self) -> requests.models.Response:
        """
        Download response.

        Parameters:
            method(str): HTTP request method.
            url (str): Data url

        Returns:
            response (requests.models.Response): Response object
        """
        logger.info('DOWNLOADING: %s', self.url)
        response = self._session.request(
            self.method, self.url, **self.requests_kwargs
        )
        response.raise_for_status()
        return response
