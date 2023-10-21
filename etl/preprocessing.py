"""Preprocessing helpers"""

class PreprocessingPipeline:

    def __init__(self):
        self._operations = []

    def add_operation(self, operation, *args, **kwargs):
        self._operations.append((operation, args, kwargs))
        return self
    
    def apply(self, data):
        result = data.copy()
        for operation, args, kwargs in self._operations:
            result = operation(result, *args, **kwargs)
        return result
