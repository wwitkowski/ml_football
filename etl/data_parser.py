from abc import ABC, abstractmethod
import logging
from typing import List
import pandas as pd

from etl.exceptions import DataParserError


logger = logging.getLogger(__name__)


class DataParser(ABC):
    """
    Abstract base class for data parsers.
    """

    @abstractmethod
    def parse(self, response: bytes) -> pd.DataFrame:
        """
        Abstract method to parse data.

        Parameters:
            response (bytes): Raw data to be parsed

        Returns:
            pd.DataFrame: Parsed data in DataFrame format

        Raises:
            DataParserError: If there's an issue during parsing
        """
        pass


class CSVDataParser(DataParser):
    """
    Parses CSV data into a Pandas DataFrame.

    Attributes:
        header (bool): Whether the CSV file has a header row.
        encoding (str): The encoding of the CSV content.
    """

    def __init__(self, header: bool = True, encoding: str = 'utf-8'):
        """
        Initialize CSVDataParser.

        Parameters:
            header (bool, optional): Whether the CSV file has a header row (default is True)
            encoding (str, optional): The encoding of the CSV content (default is 'utf-8')
        """
        self.header = header
        self.encoding = encoding

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
            DataParserError: If there's an issue during parsing
        """
        if len(content) < 2:
            logger.error('Error parsing content: Not enough content to parse.')
            raise DataParserError('Not enough content to parse')

        try:
            decoded = content.decode(self.encoding)
        except UnicodeDecodeError:
            logger.error('Error parsing content: Could not decode content.')
            raise DataParserError('Could not decode content')
        except AttributeError:
            logger.error('Error parsing content: Not a "bytes" object.')
            raise DataParserError('Content is not a "bytes" object.')

        content_lines = decoded.splitlines()
        reference_len = len(content_lines[0].split(','))
        lines = [
            parsed_line[:reference_len] for line in content_lines
            if not self._is_empty_line(parsed_line := line.split(','))
        ]
        if self.header:
            return pd.DataFrame(data=lines[1:], columns=lines[0])
        else:
            return pd.DataFrame(data=lines)
