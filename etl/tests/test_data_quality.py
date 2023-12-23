# pylint: skip-file
import pytest
from etl.exceptions import InvalidDataException
from etl.data_quality import DataQualityValidator


def test_add_condition():
    validator = DataQualityValidator()
    operation = lambda x: len(x) > 0
    validator.add_condition(operation, True)
    assert validator.conditions[0] == (operation, True, (), {})
    assert len(validator.conditions) == 1


def test_valid_data():
    validator = DataQualityValidator()
    validator.add_condition(lambda x: len(x) > 0, True)
    data = [1, 2, 3]
    assert validator.validate(data) is None


def test_invalid_data():
    validator = DataQualityValidator()
    validator.add_condition(lambda x: len(x) > 5, True)
    data = [1, 2, 3]
    with pytest.raises(InvalidDataException):
        validator.validate(data)


def test_multiple_conditions():
    validator = DataQualityValidator()
    validator.add_condition(lambda x: len(x) > 0, True)
    validator.add_condition(lambda x: all(isinstance(i, int) for i in x), True)
    data = [1, 2, 'a']
    with pytest.raises(InvalidDataException):
        validator.validate(data)