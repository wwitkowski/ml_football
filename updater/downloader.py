"""Downloaders"""

import requests
import pandas as pd


class CSVDataDownloader:
    """
    Class for handling CSV downloads from internet.

    Class also handles csv with unnecessary commas if present in file which causes read_csv() to fail.

    Methods:
        download(url, encoding, **kwargs): download csv data
    """

    @staticmethod
    def _is_empty_line(line: list[str]) -> bool:
        """
        Determine if line is empty.

        Parameters:
            line (str): List of values in line
        
        Returns:
            is_empty (bool): Whether the line is empty
        """
        if not line:
            return True
        if not any(line):
            return True
        return False

    @staticmethod
    def _parse_byte_line(line: bytes, encoding: str) -> list[str]:
        """
        Parse bytes file line and split unto list.

        Parameters:
            line (bytes): Line bytes
            encoding (str): Encoding 

        Returns:
            line (list[str]): Encoded list of str values
        """
        return line.decode(encoding).split(',')

    def _download_bad_csv_lines(self, url: str, encoding: str) -> pd.DataFrame:
        """
        Download csv using request in order to parse line by line and handle potential data issues.

        Parameters:
            url (str): Data url
            encoding (str): Encoding

        Returns:
            data (pd.DataFrame): DataFrame with downloaded data
        """
        resp = requests.get(url)
        resp.raise_for_status()
        content = resp.iter_lines()
        header = self._parse_byte_line(next(content), encoding)
        lines = [
            parsed_line[:len(header)] for line in content 
            if not self._is_empty_line(parsed_line := self._parse_byte_line(line, encoding))
        ]
        data = pd.DataFrame(data=lines, columns=header)
        return data

    def download(self, url: str, encoding: str = 'utf8', **kwargs) -> pd.DataFrame:
        """
        Download data. If pandas read_csv() fails download using requests.

        Parameters:
            url (str): Data url
            encoding (str): Encoding
            kwargs: pandas read_csv kwargs

        Returns:
            data (pd.DataFrame): DataFrame with downloaded data
        """
        try:
            data = pd.read_csv(url, encoding=encoding, **kwargs)
        except pd.errors.ParserError:
            data = self._download_bad_csv_lines(url, encoding)
        return data
