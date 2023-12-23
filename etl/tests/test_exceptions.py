# pylint: skip-file
import pytest

from etl.exceptions import InvalidDataException


def test_invalid_data_exception():
    with pytest.raises(InvalidDataException):
        raise InvalidDataException