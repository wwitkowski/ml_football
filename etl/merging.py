from abc import ABC, abstractmethod

import pandas as pd

class DataMerger(ABC):

    @abstractmethod
    def merge(datasets: list[pd.DataFrame]) -> pd.DataFrame:
        pass