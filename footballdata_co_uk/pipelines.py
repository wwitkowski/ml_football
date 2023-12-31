"""Common Pipelines"""

import pandas as pd
from etl.data_quality import DataQualityValidator
from etl.date_utils import parse_dataframe_dates
from etl.transform import TransformPipeline


def get_transform_pipeline(config) -> TransformPipeline:
    return (
        TransformPipeline()
            .add_operation(pd.DataFrame.rename, **config['rename'])
            .add_operation(
                lambda df: df[[col for col in df.columns if col in config['columns_select']]])
            .add_operation(parse_dataframe_dates, **config['parse_dates'])
            .add_operation(pd.DataFrame.replace, **config['replace'])
            .add_operation(pd.DataFrame.dropna, **config['dropna'])
            .add_operation(
                pd.DataFrame.apply,
                lambda col: pd.to_numeric(col, errors='coerce')
                if col.name in config['columns_to_numeric'] else col,
                axis=0
            )
            .add_operation(pd.DataFrame.convert_dtypes, **config['convert_dtypes'])
        )


def get_validation_pipeline(config) -> DataQualityValidator:
    return (
        DataQualityValidator()
            .add_condition(
                lambda df: all(col in df.columns for col in config['columns_required']),
                True
            )
    )
