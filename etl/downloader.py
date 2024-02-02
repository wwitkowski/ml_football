"""Downloader Objects"""
from abc import ABC, abstractmethod
import logging
from typing import Any

import requests


logger = logging.getLogger(__name__)


class Downloader(ABC):
    """
    Abstract base class defining a downloader interface.
    """

    @abstractmethod
    def download(self, session: Any | None = None) -> Any:
        """
        Abstract method to download data.

        Parameters:
            session (Any | None): Optional session to use for the download.

        Returns:
            Any: Content retrieved from the download.
        """


class APIDownloader(Downloader):
    """
    Implementation of a downloader for API endpoints.

    Attributes:
        method (str): The HTTP method used for the API request.
        url (str): The URL for the API endpoint.
        download_kwargs (dict): Additional keyword arguments for the download.
    """
    def __init__(self, method: str, url: str, **download_kwargs: dict) -> None:
        self.method = method
        self.url = url
        self.download_kwargs = download_kwargs

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
        session = session or requests.Session()
        response = session.request(self.method, self.url, **self.download_kwargs)
        response.raise_for_status()
        return response.content
