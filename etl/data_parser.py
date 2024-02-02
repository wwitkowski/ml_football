"""Data parsers"""
from abc import ABC, abstractmethod
import json
import logging
from typing import List, Dict
import pandas as pd

from etl.exceptions import DataParserException

logger = logging.getLogger(__name__)


class DataParser(ABC):
    """
    Abstract base class for data parsers.
    
    Methods:
        parse(content: bytes) -> pd.DataFrame:
            Abstract method to parse data.

        _decode_content(content: bytes, encoding: str) -> str:
            Decode content using the specified encoding.
    """

    @abstractmethod
    def parse(self, content: bytes) -> pd.DataFrame:
        """
        Abstract method to parse data.

        Parameters:
            content (bytes): Raw data to be parsed

        Returns:
            pd.DataFrame: Parsed data in DataFrame format

        Raises:
            DataParserException: If there's an issue during parsing
        """

    @staticmethod
    def _decode_content(content: bytes, encoding: str) -> str:
        """
        Decode content using the specified encoding.

        Parameters:
            content (bytes): Raw content to be decoded
            encoding (str): Encoding to be used

        Returns:
            str: Decoded content

        Raises:
            DataParserException: When error is raised during decoding
        """
        try:
            return content.decode(encoding)
        except (UnicodeDecodeError, AttributeError) as exc:
            error_msg = f'Error decoding content: {exc.__class__.__name__}'
            logger.error('Error parsing content: %s', error_msg)
            raise DataParserException(error_msg) from exc


class CSVDataParser(DataParser):
    """
    Parses CSV data into a Pandas DataFrame.

    Attributes:
        header (bool): Whether the CSV file has a header row.
        encoding (str): The encoding of the CSV content.
        delimiter (str): The delimiter used in the CSV content.
    """

    def __init__(self, header: bool = True, encoding: str = 'utf-8', delimiter: str = ','):
        """
        Initialize CSVDataParser.

        Parameters:
            header (bool, optional): Whether the CSV file has a header row (default: True)
            encoding (str, optional): The encoding of the CSV content (default: 'utf-8')
            delimiter (str, optional): The delimiter used in the CSV content (default: ',')
        """
        self.header: bool = header
        self.encoding: str = encoding
        self.delimiter: str = delimiter

    @staticmethod
    def _is_empty_line(line: List[str]) -> bool:
        """
        Check if a line contains only empty values.

        Parameters:
            line (List[str]): List of values in the line

        Returns:
            bool: Whether the line is empty
        """
        return not any(line)

    def parse(self, content: bytes) -> pd.DataFrame:
        """
        Parse CSV content into a Pandas DataFrame.

        Parameters:
            content (bytes): Raw content of CSV data

        Returns:
            pd.DataFrame: Parsed CSV data in DataFrame format, or None if parsing fails

        Raises:
            DataParserException: If there's an issue during parsing
        """
        if len(content) < 2:
            logger.error('Error parsing content: Not enough content to parse.')
            raise DataParserException('Not enough content to parse')

        decoded = self._decode_content(content, self.encoding)

        content_lines = decoded.splitlines()
        reference_len = len(content_lines[0].split(self.delimiter))
        lines = [
            parsed_line[:reference_len] for line in content_lines
            if not self._is_empty_line(parsed_line := line.split(self.delimiter))
        ]
        if self.header:
            return pd.DataFrame(data=lines[1:], columns=lines[0])
        return pd.DataFrame(data=lines)


class JSONDataParser(DataParser):
    """
    Parses JSON data into a Pandas DataFrame.

    Attributes:
        encoding (str): The encoding of the JSON content.
    """

    def __init__(self, encoding: str = 'utf-8', **kwargs: Dict):
        """
        Initialize JSONDataParser.

        Parameters:
            encoding (str, optional): The encoding of the JSON content (default is 'utf-8')
            kwargs (Dict): pandas json_normalize keyword arguments
        """
        self.encoding: str = encoding
        self.kwargs: Dict = kwargs

    def parse(self, content: bytes) -> pd.DataFrame:
        """
        Parse JSON content into a Pandas DataFrame.

        Parameters:
            content (bytes): Raw content of JSON data

        Returns:
            pd.DataFrame: Parsed JSON data in DataFrame format, or None if parsing fails

        Raises:
            DataParserException: If error when is raised during parsing JSON
        """
        decoded = self._decode_content(content, self.encoding)

        try:
            content_json = json.loads(decoded)
        except json.JSONDecodeError as exc:
            logger.error('Error parsing content: Could not decode JSON content.')
            raise DataParserException('Could not decode JSON content') from exc

        return pd.json_normalize(content_json, **self.kwargs)
