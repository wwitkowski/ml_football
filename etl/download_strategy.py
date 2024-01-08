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


class ReplaceOnMetaFlagStrategy(DownloadStrategy):
    """
    Download strategy for replacing data based on metadata flag.

    This strategy checks if the 'replace' flag in download object metadata is True 
    or if the file doesn't exist.
    """
    def is_download_required(self, obj: Downloader) -> bool:
        """
        Determines whether a download is required for a 'replace on meta flag' strategy.

        Args:
            obj (Downloader): The object representing the downloader

        Returns:
            bool: True if 'replace' flag in metadata is True or file does not exist, False otherwise
        """
        return not obj.file.exists() or obj.meta.get('replace', False)