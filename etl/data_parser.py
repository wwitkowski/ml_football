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
            response (bytes): Data to be parsed
        
        Returns:
            pd.DataFrame: Parsed data in DataFrame format

        Raises:
            DataParserError: If there's an issue parsing the data
        """
        pass

class CSVDataParser(DataParser):
    """
    Parses CSV data into a Pandas DataFrame.
    """
    def __init__(self, header: bool = True, encoding: str = 'utf-8'):
        self.header = header
        self.encoding = encoding

    @staticmethod
    def _is_empty_line(line: List[str]) -> bool:
        """
        Determine if line is empty.
        
        Parameters:
            line (List[str]): List of values in line
        
        Returns:
            bool: Whether the line is empty
        """
        return not any(line)
    
    def parse(self, content: bytes) -> pd.DataFrame:
        """
        Parse CSV content and convert it into a DataFrame.
        
        Parameters:
            content (bytes): Raw content of CSV data
        
        Returns:
            pd.DataFrame: Parsed CSV data in DataFrame format or None if parsing fails

        Raises:
            DataParserError: If there's an issue parsing the data
        """
        if len(content) < 2:
            logger.error('Error parsing data: Not enough content to parse.')
            raise DataParserError('Not enough content to parse')
        try:
            decoded = content.decode(self.encoding)
        except UnicodeDecodeError:
            logger.error('Error parsing data: Could not decode data content.')
            raise DataParserError('Could not decode data content')

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
