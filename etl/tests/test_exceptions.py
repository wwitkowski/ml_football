# pylint: skip-file
import pytest

from etl.exceptions import DataParserException, InvalidDataException


def test_invalid_data_exception():
    with pytest.raises(InvalidDataException):
        raise InvalidDataException
    
def test_data_perser_exception():
    with pytest.raises(DataParserException):
        raise DataParserException