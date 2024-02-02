"""Data objects"""
from dataclasses import dataclass, field
from typing import Dict

from etl.downloader import Downloader
from etl.files import File
from etl.uploader import Uploader


@dataclass
class DataObject:
    """
    Represents a data object for downloading, storing and uploading dataset.

    Attributes:
        downloader (Downloader): The downloader responsible for fetching data.
        file (File): The file associated with the data.
        uploader (Uploader): The uploader responsible for uploading data.
        meta (Dict): A dictionary for storing additional metadata.
    """
    downloader: Downloader
    file: File
    uploader: Uploader
    meta: Dict = field(default_factory=dict)
