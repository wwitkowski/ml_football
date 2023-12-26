import logging
from typing import Any, Callable, List
from etl.exceptions import InvalidDataException


logger = logging.getLogger(__name__)


class DataQualityValidator:
    """
    Validates data based on specified conditions.

    Attributes:
        conditions (list): List containing tuples of validation conditions, expected results, arguments, and keyword arguments.
    """
    def __init__(self) -> None:
        self.conditions: List[tuple] = []

    def add_condition(
        self, condition: Callable[..., bool], result: bool, *args: Any, **kwargs: Any
    ) -> 'DataQualityValidator':
        """
        Adds a condition to be checked during validation.
        
        Parameters:
            condition (callable): The validation condition to check.
            result (bool): The expected result of the condition.
            *args: Condition function arguments.
            **kwargs: Condition function keyword arguments.
        
        Returns:
            DataQualityValidator: The instance of DataQualityValidator with the added condition.
        """
        self.conditions.append((condition, result, args, kwargs))
        return self
    
    def validate(self, data: Any) -> None:
        """
        Validates the data based on the added conditions.
        
        Parameters:
            data (any): Data to be validated.
        
        Raises:
            InvalidDataException: If any condition fails during validation.
        """
        for condition, expected_result, args, kwargs in self.conditions:
            result = condition(data, *args, **kwargs) 
            if expected_result == result:
                continue
            else:
                logger.warning(
                    'Validation failed for condition: %s. Expected result: %s, got %s',
                    condition.__name__, expected_result, result
                )
                raise InvalidDataException(
                    f'Validation failed for condition: {condition.__name__}. Expected result: {expected_result}, got {result}'
                )
