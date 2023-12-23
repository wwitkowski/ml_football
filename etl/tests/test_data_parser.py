# pylint: skip-file
import pandas as pd
from etl.data_parser import CSVDataParser




def test_csv_parser_valid_data() -> None:
    content = b'col1,col2\n1,4\n2,5\n3,6'
    expected_data = pd.DataFrame({'col1': ['1', '2', '3'], 'col2': ['4', '5', '6']})
    parser = CSVDataParser()
    parsed = parser.parse(content)
    pd.testing.assert_frame_equal(parsed, expected_data)


def test_csv_parser_invalid_data() -> None:
    content = b'col1,col2,col3\n1,2,3\n4,5,6,7\n,,\n'
    expected_data = pd.DataFrame({'col1': ['1', '4'], 'col2': ['2', '5'], 'col3': ['3', '6']})
    parser = CSVDataParser()
    parsed = parser.parse(content)
    print(parsed)
    pd.testing.assert_frame_equal(parsed, expected_data)


def test_csv_parser_incomplete_data() -> None:
    content = b'col1,col2,col3\n1,2,3\n4,5,6\n7,,8,\n9,10'
    expected_data = pd.DataFrame(
        {'col1': ['1', '4', '7', '9'], 'col2': ['2', '5', '', '10'], 'col3': ['3', '6', '8', None]}
    )
    parser = CSVDataParser()
    parsed = parser.parse(content)
    pd.testing.assert_frame_equal(parsed, expected_data)


def test_csv_parser_no_header():
    content = b'1,4\n2,5\n3,6'
    expected_data = pd.DataFrame(
        [['1', '4'], ['2', '5'], ['3', '6']])
    parser = CSVDataParser(header=False)
    parsed = parser.parse(content)
    pd.testing.assert_frame_equal(parsed, expected_data)
    
