import pandas as pd


class PandasDatasetValidator:

    def __init__(self, config):
        self.config = config

    def _validate_columns(self, data):
        assert all([col in data.columns for col in self.config['columns']['valid_columns']])

    def _validate_rows(self, data):
        if threshold:=self.config['rows'].get('thresholds'):
            data = data.dropna(thresh=int(threshold*len(data.columns)))
        data = data.dropna(subset=self.config['columns']['valid_columns'])
        return data

    def _validate_dtypes(self, data):
        for col, dtype in self.config['valid_dtypes'].items():
            if col not in data.columns:
                continue
            if dtype in ('int64', 'float64'):
                data.loc[:, col] = pd.to_numeric(data[col], errors='coerce')
        return data
    
    def validate(self, data):
        self._validate_columns(data)
        data = self._validate_rows(data)
        data = self._validate_dtypes(data)
        return data