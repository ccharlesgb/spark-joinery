from collections.abc import Sequence
from typing import TypeVar
from pyspark.sql import DataFrame, SparkSession
from pyspark_schemas import schemas

T = TypeVar("T")


def get_dataframe(
    spark: SparkSession, row_type: type[T], rows: Sequence[T]
) -> DataFrame:
    for index, row in enumerate(rows):
        if not isinstance(row, row_type):
            raise ValueError(
                f"Row {index} of type {row.__class__.__name__}. Expected type {row_type.__name__}"
            )
    schema = schemas.get_spark_schema_from_model(row_type)

    serialized_rows = [
        row.model_dump(mode="python") if hasattr(row, "model_dump") else row
        for row in rows
    ]
    return spark.createDataFrame(serialized_rows, schema)
