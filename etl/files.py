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
        Initialize class

        Parameters:
            path (str | Path): File path

        Returns:
            None
        """
        self.path = Path(path)

    def exists(self) -> bool:
        """
        Check if file exists.

        Returns:
            bool: Whether file exists
        """
        return self.path.is_file()

    def read(self, mode: str = 'rb') -> bytes | str:
        """
        Read file.

        Parameters:
            mode (str): File open mode ('r', 'rb', 'r+', etc.)

        Returns:
            content(bytes | str): Content read from the file
        """
        logger.info('Reading data from file %s.', self.path)
        try:
            with open(self.path, mode) as f:
                content = f.read()
        except FileNotFoundError:
            logger.error('File %s not found.', self.path)
            raise
        except IOError as e:
            logger.error('Error reading file %s: %s', self.path, e)
            raise
        return content

    def save(self, content: bytes | str, mode: str = 'wb') -> None:
        """
        Save data to a file.

        Parameters:
            content (bytes | str): Data to be written to the file
            mode (str): File open mode ('w', 'wb', 'w+', etc.)

        Returns:
            None
        """
        self.path.parent.mkdir(parents=True, exist_ok=True)
        try:
            with open(self.path, mode) as f:
                f.write(content)
        except IOError as e:
            logger.error('Error writing to file %s: %s', self.path, e)
            raise
