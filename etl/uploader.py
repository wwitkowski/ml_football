"""Data uploaders"""
from abc import ABC, abstractmethod
from typing import Any, Optional

import pandas as pd


class Uploader(ABC):
    """
    Abstract base class defining an uploader interface.
    """

    @abstractmethod
    def upload(self, data: Any, session: Optional[Any] = None) -> None:
        """
        Abstract method to upload data.

        Parameters:
            data (Any): Data to be uploaded.
            session (Any | None): Optional session to use for the upload.

        Returns:
            None
        """

class DatabaseUploader(Uploader):
    """
    Uploads data to a database.

    Attributes:
        schema (str): The database schema name.
        table (str): The table name.
        on_constraint (str | None): The conflict resolution strategy ('update', 'pass', or None).
        constraint (str | None): The constraint name to act on.
    """

    def __init__(
            self,
            schema: str,
            table: str,
            on_constraint: Optional[str] = None,
            constraint: Optional[str] = None
        ) -> None:
        self.schema: str = schema
        self.table: str = table
        if any([
            on_constraint is None and constraint is not None,
            on_constraint is not None and constraint is None
        ]):
            raise ValueError('None or both of arguments (contrains, on_constraint) must be provided')
        self.on_constraint: Optional[str] = on_constraint
        self.constraint: Optional[str] = constraint

    def upload(self, data: pd.DataFrame, session: Any) -> None:
        """
        Upload data to the database.

        Parameters:
            data (pd.DataFrame): Data to be uploaded.
            session (Any): Database session.

        Returns:
            None
        """
        placeholders = ', '.join([':' + col for col in data.columns])
        columns = ', '.join(data.columns)
        query = f"INSERT INTO {self.schema}.{self.table} ({columns}) VALUES ({placeholders}) "
        if self.on_constraint == 'update':
            query += (
                f"ON CONFLICT ON CONSTRAINT {self.constraint} DO UPDATE SET "
                f"{', '.join(f'{col} = EXCLUDED.{col}' for col in data.columns)}"
            )
        elif self.on_constraint == 'pass':
            query += f"ON CONFLICT ON CONSTRAINT {self.constraint} DO NOTHING"
        session.execute(query, data.to_dict(orient='records'))
