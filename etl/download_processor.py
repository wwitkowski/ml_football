"""Download processors"""
from abc import ABC, abstractclassmethod
import logging

from etl.data_parser import BaseParser, DataParser
from etl.data_quality import DataQualityValidator
from etl.downloader import DataDownloader

from etl.files import FileManager
from etl.preprocessing import PreprocessingPipeline


logger = logging.getLogger(__name__)


class DownloadProcessor(ABC):

    @abstractclassmethod
    def process_download(self):
        pass


class DownloadProcessor:

    def __init__(self, reload: bool):
        self.reload = reload

    def process(
            self,
            downloader: DataDownloader,
            file_manager = FileManager,
            parser: DataParser | None = None,
            preprocessing_pipeline: PreprocessingPipeline | None = None, 
            validator: DataQualityValidator | None = None, 
        ):
        parser = parser or BaseParser()
        if file_manager.exists() and self.reload is False:
            data = file_manager.read()
        else:
            response = downloader.download()
            data = parser.parse(response)
            if validator:
                validator.validate(data)
            file_manager.save(data)
        if preprocessing_pipeline:
            data = preprocessing_pipeline.apply(data)
        return data
