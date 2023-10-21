"""File managers"""

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any
import logging
import pandas as pd


logger = logging.getLogger(__name__)


class FileManager(ABC):
    """Abstract class for file managing"""
    path: Path

    @abstractmethod
    def exists(self) -> bool: # pragma: no cover
        """Check if file exists"""

    @abstractmethod
    def read(self) -> Any: # pragma: no cover
        """Read file"""

    @abstractmethod
    def save(self, data: Any) -> None: # pragma: no cover
        """Save file"""


class CSVFileManager(FileManager):
    """
    Class for managing csv files.

    Attributes:
        path (str or Path): Path where the file is saved/read

    Methods:
        exists(): Check if the file exists
        read(**kwargs): Read file
        save(data, **kwargs): Save data to a file
    """

    def __init__(self, path: str | Path, index: bool = False) -> None:
        """
        Initialize class

        Parameters:
            path (str or Path): File path
            index (bool): Whether to save a dataframe index along with the data

        Returns:
            None
        """
        self.path = path if isinstance(path, Path) else Path(path)
        self.index = index

    def exists(self) -> bool:
        """
        Check if file exists.

        Returns:
            exists (bool): Whether file exists
        """
        return self.path.is_file()

    def read(self) -> pd.DataFrame:
        """
        Read file.

        Parameters:
            kwargs (Callable[P, Any]): Pandas read_csv() keyword arguments

        Returns:
            data (pd.DataFrame): File data
        """
        logger.info('Reading data from file.')
        return pd.read_csv(self.path)

    def save(self, data: pd.DataFrame) -> None:
        """
        Save data to a file.

        Parameters:
            data (pd.DataFrame): Data to be saved to a file
            kwargs (Callable[P, Any]): Pandas to_csv() method keyword arguments

        Returns:
            None
        """
        self.path.parent.mkdir(parents=True, exist_ok=True)
        data.to_csv(self.path, index=self.index)
