"""Data parsers"""

from abc import ABC, abstractmethod

import requests
import pandas as pd


class DataParser(ABC):

    @abstractmethod
    def parse(response: requests.models.Response):
        pass


class BaseParser(DataParser):

    def parse(response: requests.models.Response):
        return response.json()


class CSVDataParser(DataParser):

    def __init__(self, encoding='utf-8'):
        self.encoding = encoding

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
    
    def parse(self, response: requests.models.Response):
        content = response.iter_lines()
        header = self._parse_byte_line(next(content))
        lines = [
            parsed_line[:len(header)] for line in content
            if not self._is_empty_line(parsed_line := self._parse_byte_line(line))
        ]
        data = pd.DataFrame(data=lines, columns=header)
        return data