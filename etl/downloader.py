"""Downloaders"""

from abc import ABC, abstractmethod
import logging
import time
from typing import Callable, ParamSpec, Any
from urllib3.exceptions import SSLError

import requests
import pandas as pd


logger = logging.getLogger(__name__)
P = ParamSpec("P")


class DataDownloader(ABC):
    """Abstract DataDownloader class"""

    @abstractmethod
    def download(self, url: str, **kwargs: Callable[P, Any]) -> Any: # pragma: no cover
        """abstract download function"""


class CSVRequestsDataDownloader:
    """
    Class for handling CSV downloads from internet.

    Class also handles csv with unnecessary commas if present in file which causes read_csv() to fail.

    Methods:
        download(url, encoding, **kwargs): Download csv data
    """

    def __init__(self, encoding: str = 'utf-8') -> None:
        """
        Init csv downloader class.

        Parameters:
            encoding (str): Bytes encoding method

        Returns:
            None
        """
        self.encoding = encoding
        self._session = requests.Session()

    @staticmethod
    def _is_empty_line(line: list[str]) -> bool:
        """
        Determine if line is empty.

        Parameters:
            line (str): List of values in line
        
        Returns:
            is_empty (bool): Whether the line is empty
        """
        if not any(line):
            return True
        return False

    def _parse_byte_line(self, line: bytes) -> list[str]:
        """
        Parse bytes file line and split unto list.

        Parameters:
            line (bytes): Line bytes

        Returns:
            line (list[str]): Encoded list of str values
        """
        return line.decode(self.encoding).split(',')

    def download(self, url: str) -> pd.DataFrame:
        """
        Download csv using request in order to parse line by line and handle potential data issues.

        Parameters:
            url (str): Data url

        Returns:
            data (pd.DataFrame): DataFrame with downloaded data
        """
        resp = self._session.get(url)
        resp.raise_for_status()
        content = resp.iter_lines()
        header = self._parse_byte_line(next(content))
        lines = [
            parsed_line[:len(header)] for line in content
            if not self._is_empty_line(parsed_line := self._parse_byte_line(line))
        ]
        data = pd.DataFrame(data=lines, columns=header)
        return data
