"""Download strategies"""
from abc import ABC, abstractmethod
from etl.downloader import Downloader


class DownloadStrategy(ABC):
    """
    Abstract base class defining a download strategy interface.
    """
    @abstractmethod
    def is_download_required(self, obj: Downloader) -> bool:
        """
        Determine whether a download is required based on specific conditions.

        Args:
            obj (Downloader): The object representing the downloader

        Returns:
            bool: True if download is required, False otherwise
        """


class AppendStrategy(DownloadStrategy):
    """
    Download strategy for appending data.

    This strategy checks if the file already exists and returns True if it needs to be downloaded.
    """
    def is_download_required(self, obj: Downloader) -> bool:
        """
        Determines whether a download is required for an 'append' strategy.

        Args:
            obj (Downloader): The object representing the downloader

        Returns:
            bool: False if file exists, otherwise True
        """
        print(obj.file.exists())
        return not obj.file.exists()


class ReplaceStrategy(DownloadStrategy):
    """
    Download strategy for replacing data.

    This strategy always returns True to always download the data from source.
    """
    def is_download_required(self, obj: Downloader) -> bool:
        """
        Determines whether a download is required for a 'replace' strategy.

        Args:
            obj (Downloader): The object representing the downloader

        Returns:
            bool: Always returns True
        """
        return True
