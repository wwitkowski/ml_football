# pylint: skip-file
from unittest.mock import MagicMock
import pytest
import pandas as pd
from etl.uploader import DatabaseUploader


def test_upload():
    data = pd.DataFrame({'col1': [1, 2], 'col2': ['a', 'b']})
    mock_session = MagicMock()

    uploader = DatabaseUploader('test_schema', 'test_table')
    uploader.upload(data, mock_session)

    expected_query = (
        'INSERT INTO test_schema.test_table (col1, col2) VALUES (:col1, :col2) '
    )
    mock_session.execute.assert_called_once_with(
        expected_query, 
        [
            {'col1': 1, 'col2': 'a'},
            {'col1': 2, 'col2': 'b'}
        ]
    )


def test_upload_pass_on_constraint():
    data = pd.DataFrame({'col1': [1, 2], 'col2': ['a', 'b']})
    mock_session = MagicMock()

    uploader = DatabaseUploader(
        'test_schema', 
        'test_table', 
        constraint='test_constraint',
        on_constraint='pass'
    )
    uploader.upload(data, mock_session)

    expected_query = (
        'INSERT INTO test_schema.test_table (col1, col2) VALUES (:col1, :col2) '\
        'ON CONFLICT ON CONSTRAINT test_constraint DO NOTHING'
    )
    mock_session.execute.assert_called_once_with(
        expected_query, 
        [
            {'col1': 1, 'col2': 'a'},
            {'col1': 2, 'col2': 'b'}
        ]
    )


def test_upload_update_on_constraint():
    data = pd.DataFrame({'col1': [1, 2], 'col2': ['a', 'b']})
    mock_session = MagicMock()

    uploader = DatabaseUploader(
        'test_schema', 
        'test_table', 
        constraint='test_constraint',
        on_constraint='update'
    )
    uploader.upload(data, mock_session)

    expected_query = (
        'INSERT INTO test_schema.test_table (col1, col2) VALUES (:col1, :col2) '\
        'ON CONFLICT ON CONSTRAINT test_constraint DO UPDATE SET '\
        'col1 = EXCLUDED.col1, col2 = EXCLUDED.col2'
    )
    mock_session.execute.assert_called_once_with(
        expected_query, 
        [
            {'col1': 1, 'col2': 'a'},
            {'col1': 2, 'col2': 'b'}
        ]
    )


def test_upload_invalid_init():
    with pytest.raises(ValueError):
        uploader = DatabaseUploader(
            'test_schema', 
            'test_table', 
            constraint='test_constraint',
        )

    with pytest.raises(ValueError):
        uploader = DatabaseUploader(
            'test_schema', 
            'test_table', 
            on_constraint='pass',
        )