"""Downlaod strategies"""
from abc import ABC, abstractmethod
from etl.downloader import DownloaderObject

from etl.files import File


class DownloadStrategy(ABC):
    @abstractmethod
    def is_download_required(self, obj: DownloaderObject, file: File) -> bool:
        """
        Determine whether a download is required based on specific conditions.

        Args:
            obj (DownloaderObject): The object representing the downloader
            file (File): File manager

        Returns:
            bool: True if download is required, False otherwise
        """


class AppendStrategy(DownloadStrategy):
    def is_download_required(self, obj: DownloaderObject, file: File) -> bool:
        """
        Determines whether a download is required for an 'append' strategy.

        Args:
            obj (DownloaderObject): The object representing the downloader
            file (File): File manager

        Returns:
            bool: False if file exists, otherwise True
        """
        return not file.exists()


class ReplaceStrategy(DownloadStrategy):
    def is_download_required(self, obj: DownloaderObject, file: File) -> bool:
        """
        Determines whether a download is required for a 'replace' strategy.

        Args:
            obj (DownloaderObject): The object representing the downloader
            file (File): File manager

        Returns:
            bool: Always returns True
        """
        return True
    

class ReplaceOnMetaFlagStrategy(DownloadStrategy):
    def is_download_required(self, obj: DownloaderObject, file: File) -> bool:
        """
        Determines whether a download is required for a 'replace on meta flag' strategy.

        Args:
            obj (DownloaderObject): The object representing the downloader
            file (File): The file object to check against

        Returns:
            bool: True if 'replace' flag in metadata is True or file does not exists, False otherwise
        """
        print(obj.file_path)
        print(obj.meta.get('replace', False))
        return not file.exists() or obj.meta.get('replace', False)