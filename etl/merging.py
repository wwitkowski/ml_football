"""Datasets mergers"""

from abc import ABC, abstractmethod
import pandas as pd


class DataMerger(ABC):
    """ABstract DataMerger class"""

    @abstractmethod
    def merge(self, datasets: list[pd.DataFrame]) -> pd.DataFrame: # pragma: no cover
        """abstract merge function"""
