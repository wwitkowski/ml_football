import pytest
import numpy as np
import pandas as pd
from updater.validator import PandasDatasetValidator


data = pd.DataFrame({
    'A': [1, 2, 3, np.nan, np.nan],
    'B': [4, 5, 10, 7, np.nan],
    'C': [np.nan, 8.5, 9.3, 10.1, np.nan],
    'D': [np.nan, 'val2', 'val3', np.nan, np.nan],
    'E': [np.nan, 4, 3, 'val4', np.nan]
})

# Test configuration
cols_config = {
    'columns': {
        'valid_columns': ['A', 'B']
    }
}
threshold_config = {
    'rows': {
        'threshold': 0.5
    }
}
dtypes_config = {
    'valid_dtypes': {
        'A': 'float64',
        'B': 'int64',
        'C': 'float64',
        'E': 'numeric'
    },
}


def test_pandas_dataset_validator_init():
    val = PandasDatasetValidator(cols_config)
    assert val.config == cols_config


# Test cases
def test_validate_columns():
    validator = PandasDatasetValidator(cols_config)
    # Test case 1: Valid columns should pass the validation
    validated_data = validator.validate(data)
    expected_data = data.drop(index=[3, 4])
    assert validated_data.sort_index().equals(expected_data.sort_index())

    # Test case 2: Missing column 'A' should raise AssertionError
    invalid_data = data.drop(columns=['A'])
    with pytest.raises(AssertionError):
        validator.validate(invalid_data)

def test_validate_rows():
    validator = PandasDatasetValidator(threshold_config)
    # Test case 1: Rows with more than 50% NA values should be dropped
    validated_data = validator.validate(data)
    expected_data = data.drop(index=[0, 4])
    assert validated_data.sort_index().equals(expected_data.sort_index())

def test_validate_dtypes():
    validator = PandasDatasetValidator(dtypes_config)
    # Test case 1: Columns 'A' and 'B' should be converted to int64
    validated_data = validator.validate(data)
    expected_data = data.drop(index=4)
    expected_data = expected_data.astype({'A': 'float64', 'B': 'int64', 'C': 'float64'})
    expected_data['E'] = pd.to_numeric(expected_data['E'], errors='coerce')
    assert validated_data.sort_index().equals(expected_data.sort_index())
