import requests
import pandas as pd


class CSVDataDownloader:

    @staticmethod
    def _is_empty_line(line):
        if not line:
            return True
        if not any([value for value in line]):
            return True
        return False

    @staticmethod
    def _parse_byte_line(line, encoding):
        return line.decode(encoding).split(',')

    def _download_bad_csv_lines(self, url, encoding):
        r = requests.get(url)
        r.raise_for_status()
        content = r.iter_lines()
        header = self._parse_byte_line(next(content), encoding)
        lines = [
            parsed_line[:len(header)] for line in content 
            if not self._is_empty_line(parsed_line := self._parse_byte_line(line, encoding))
        ]
        data = pd.DataFrame(data=lines, columns=header)
        return data

    def download(self, url, encoding='utf8', **kwargs):
        try:
            data = pd.read_csv(url, encoding=encoding, **kwargs)
        except pd.errors.ParserError:
            data = self._download_bad_csv_lines(url, encoding)
        return data