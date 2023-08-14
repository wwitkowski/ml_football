"""File managers"""

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Callable, ParamSpec
import pandas as pd


P = ParamSpec("P")


class File(ABC):
    """Abstract class for file managing"""

    @abstractmethod
    def exists(): # pragma: no cover
        """Check if file exists"""

    @abstractmethod
    def read(**kwargs: Callable[P, Any]): # pragma: no cover
        """Read file"""

    @abstractmethod
    def save(data, **kwargs: Callable[P, Any]): # pragma: no cover
        """Save file"""


class CSVFile:
    """
    Class for managing csv files.

    Attributes:
        path (str or Path): Path where the file is saved/read

    Methods:
        exists(): Check if the file exists
        read(**kwargs): Read file
        save(data, **kwargs): Save data to a file
    """

    def __init__(self, path: str | Path) -> None:
        """
        Initialize class

        Parameters:
            path (str or Path): File path

        Returns:
            None
        """
        self.path = path if isinstance(path, Path) else Path(path)

    def exists(self) -> bool:
        """
        Check if file exists.

        Returns:
            exists (bool): Whether file exists
        """
        return self.path.is_file()

    def read(self, **kwargs: Callable[P, Any]):
        """
        Read file.

        Parameters:
            kwargs (Callable[P, Any]): Pandas read_csv() keyword arguments

        Returns:
            data (pd.DataFrame): File data
        """
        return pd.read_csv(self.path, **kwargs)
    
    def save(self, data: pd.DataFrame, **kwargs: Callable[P, Any]) -> None:
        """
        Save data to a file.

        Parameters:
            data (pd.DataFrame): Data to be saved to a file
            kwargs (Callable[P, Any]): Pandas to_csv() method keyword arguments

        Returns:
            None
        """
        self.path.mkdir(parents=True, exist_ok=True)
        data.to_csv(self.path, **kwargs)
