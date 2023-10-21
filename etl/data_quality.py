"""Data quality validators"""

from etl.exceptions import NotValidDataException


class DataQualityValidator:

    def __init__(self):
        self.conditions = []

    def add_condition(self, condition, result, *args, **kwargs):
        self.conditions.append((condition, result, args, kwargs))
        return self
    
    def validate(self, data):
        for condition, result, args, kwargs in self.conditions:
            if condition(data, *args, **kwargs) == result:
                continue
            else:
                raise NotValidDataException
