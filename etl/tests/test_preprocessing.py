import pytest
from datetime import datetime

import pandas as pd
import numpy as np

from etl.preprocessing import parse_dates


testing_df = pd.DataFrame({
    'column': ['A', 'B', 'C', 'D'],
    'date': ['2023-08-01', '2023-08-02', '2023-08-03', np.nan,]
})


def test_parse_dates_valid_format():
    expected_df = pd.DataFrame({
        'column': ['A', 'B', 'C', 'D'],
        'date': [datetime(2023, 8, 1), datetime(2023, 8, 2), datetime(2023, 8, 3), np.nan],
    })
    result_df = parse_dates(testing_df, 'date', ['%Y/%m/%d', '%Y-%m-%d'])
    pd.testing.assert_frame_equal(
        result_df.reset_index(drop=True),
        expected_df.reset_index(drop=True)
    )


def test_parse_dates_invalid_format():
    with pytest.raises(pd.errors.ParserError):
        result_df = parse_dates(testing_df, 'date', ['%Y/%m/%d'])
