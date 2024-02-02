"""Common Pipelines"""

import pandas as pd
from etl.data_quality import DataQualityValidator
from etl.date_utils import parse_dataframe_dates
from etl.transform import TransformPipeline


def get_transform_schedule_pipeline(config) -> TransformPipeline:
    return (
        TransformPipeline()
            .add_operation(pd.DataFrame.rename, **config['rename'])
            .add_operation(
                lambda df: df[[col for col in df.columns if col in config['columns_select']]])
    )


def get_validation_pipeline(config) -> DataQualityValidator:
    return (
        DataQualityValidator()
            .add_condition(
                lambda df: all(col in df.columns for col in config['columns_required']),
                True
            )
    )
