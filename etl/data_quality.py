from typing import Any, Callable
from etl.exceptions import InvalidDataException


class DataQualityValidator:
    """
    Validates data based on specified conditions.
    """
    def __init__(self):
        self.conditions = []

    def add_condition(
        self, condition: Callable[..., bool], result: bool, *args: Any, **kwargs: Any
    ) -> 'DataQualityValidator':
        """
        Adds a condition to be checked during validation.
        
        Parameters:
            condition (callable): The validation condition to check
            result (bool): The expected result of the condition
            *args: condition function args
            **kwargs: condition function kwargs
        
        Returns:
            DataQualityValidator: The instance of DataQualityValidator with the added condition
        """
        self.conditions.append((condition, result, args, kwargs))
        return self
    
    def validate(self, data: Any) -> None:
        """
        Validates the data based on the added conditions.
        
        Parameters:
            data (any): Data to be validated
        
        Raises:
            InvalidDataException: If any condition fails during validation
        """
        for condition, expected_result, args, kwargs in self.conditions:
            result = condition(data, *args, **kwargs) 
            if expected_result == result:
                continue
            else:
                raise InvalidDataException(
                    'Validation failed for condition: %s. Expected result: %s, got %s', 
                    condition.__name__, expected_result, result
                )
