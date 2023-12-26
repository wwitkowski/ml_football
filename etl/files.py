"""Custom File Managers"""
from pathlib import Path
import logging

logger = logging.getLogger(__name__)


class File:
    """
    Class for managing files.

    Attributes:
        path (Path): The path to the file.
    """

    def __init__(self, path: str | Path) -> None:
        """
        Initialize class.

        Parameters:
            path (str | Path): File path.

        Returns:
            None
        """
        self.path = Path(path)

    def exists(self) -> bool:
        """
        Check if file exists.

        Returns:
            bool: Whether file exists.
        """
        return self.path.is_file()

    def read(self, mode: str = 'rb', encoding=None) -> bytes:
        """
        Read file. Currently only mode that reads bytes is supported.

        Parameters:
            mode (str): File open mode ('r', 'rb', 'r+', etc.).

        Returns:
            content(bytes | str): Content read from the file.

        Raises:
            NotImplementedError: If the mode is unsupported.
            FileNotFoundError: If the file does not exist.
            IOError: If an error occurs while reading the file.
        """
        logger.info('Reading data from file %s.', self.path)
        if mode != 'rb':
            raise NotImplementedError('Currently only mode that reads bytes is supported.')
        try:
            with open(self.path, mode, encoding=encoding) as f:
                content = f.read()
        except FileNotFoundError:
            logger.error('File %s not found.', self.path)
            raise
        except IOError as exc:
            logger.error('Error reading file %s: %s', self.path, exc)
            raise
        return content

    def save(self, content: bytes | str, mode: str = 'wb', encoding='utf-8') -> None:
        """
        Save data to a file.

        Parameters:
            content (bytes | str): Data to be written to the file.
            mode (str): File open mode ('w', 'wb', 'w+', etc.).

        Returns:
            None

        Raises:
            IOError: If an error occurs while writing to the file.
        """
        self.path.parent.mkdir(parents=True, exist_ok=True)
        try:
            with open(self.path, mode, encoding=encoding) as f:
                f.write(content)
        except IOError as exc:
            logger.error('Error writing to file %s: %s', self.path, exc)
            raise
