from abc import ABC, abstractmethod
import logging
import requests


logger = logging.getLogger(__name__)


class Downloader(ABC):
    """
    Abstract base class defining a downloader interface.
    """
    @abstractmethod
    def download(self, session: requests.Session | None = None) -> bytes:
        """
        Abstract method to download data.

        Parameters:
            session (Optional[requests.Session]): Optional requests session to use for the download.

        Returns:
            bytes: Content retrieved from the download.
        """
        pass


class APIDownloader(Downloader):
    """
    Implementation of a downloader for API endpoints.
    """
    def __init__(self, method: str, url: str, file_path: str, table: str, schema: str, **download_kwargs):
        self.method = method
        self.url = url
        self.file_path = file_path
        self.table = table
        self.schema = schema
        self.download_kwargs = download_kwargs
    
    def download(self, session: requests.Session | None = None) -> bytes:
        """
        Download data from the specified URL using the provided method and options.

        Parameters:
            session (requests.Session | None): Optional requests session to use for the download.

        Returns:
            bytes: Content retrieved from the download.

        Raises:
            requests.HTTPError: If the response status code is not a success code.
        """
        logger.info('DOWNLOADING: %s', self.url)
        session = session or requests.Session()
        response = session.request(self.method, self.url, **self.download_kwargs)
        response.raise_for_status()
        return response.content
