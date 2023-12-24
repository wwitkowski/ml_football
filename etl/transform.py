from typing import Any, Callable

class TransformPipeline:
    """
    Class managing a pipeline of transformation operations.

    Attributes:
        _operations (list): List containing tuples of operations, arguments, and keyword arguments.
    """

    def __init__(self) -> None:
        self._operations = []

    def add_operation(self, operation: Callable, *args: Any, **kwargs: Any) -> 'TransformPipeline':
        """
        Add an operation to the transformation pipeline.

        Parameters:
            operation (Callable): Operation to add.
            *args (Any): Arguments for the operation.
            **kwargs (Any): Keyword arguments for the operation.

        Returns:
            TransformPipeline: Updated instance with the added operation.
        """
        self._operations.append((operation, args, kwargs))
        return self

    def apply(self, data: Any) -> Any:
        """
        Apply the sequence of operations to the provided data.

        Parameters:
            data (Any): Data to apply the operations on.

        Returns:
            data(Any): Transformed data after applying all operations.
        """
        for operation, args, kwargs in self._operations:
            data = operation(data, *args, **kwargs)
        return data

    def copy(self) -> 'TransformPipeline':
        """
        Create a copy of the current pipeline.

        Returns:
            TransformPipeline: Copy of the current pipeline.
        """
        pipe = TransformPipeline()
        for op, args, kwargs in self._operations:
            pipe.add_operation(op, *args, **kwargs)
        return pipe
