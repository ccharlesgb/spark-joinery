import datetime
import decimal
from typing import Annotated, Literal, Optional

from pyspark_schemas import type_inspection
from pyspark.sql import types
import pytest


@pytest.mark.parametrize(
    "python_type, expected_spark_type",
    [
        (int, types.IntegerType()),
        (str, types.StringType()),
        (float, types.FloatType()),
        (bool, types.BooleanType()),
        (Optional[int], types.IntegerType()),
        (int | None, types.IntegerType()),
        (Annotated[int, types.ShortType()], types.ShortType()),
        (Annotated[int, types.IntegerType()], types.IntegerType()),
        (Annotated[int, types.StringType()], types.StringType()),
        (Literal["a", "b"], types.StringType()),
        (Literal[1, 2], types.IntegerType()),
        (Literal[True, False], types.BooleanType()),
        (Annotated[Literal[1, 2], types.IntegerType()], types.IntegerType()),
        (Annotated[Literal[1, 2], types.StringType()], types.StringType()),
        (datetime.datetime, types.TimestampType()),
        (datetime.date, types.DateType()),
        (
            decimal.Decimal,
            types.DecimalType(
                type_inspection.SPARK_MAX_DECIMAL_PRECISION,
                type_inspection.DEFAULT_FRACTIONAL_DIGITS,
            ),
        ),
        (bytes, types.BinaryType()),
    ],
)
def test_get_spark_type_from_python_type(python_type, expected_spark_type):
    actual_spark_type = type_inspection.get_spark_type_from_python_type(python_type)
    assert actual_spark_type == expected_spark_type


def test_get_spark_type_from_python_type_raises_value_error_for_mixed_literal_types():
    with pytest.raises(
        ValueError,
        match=r"Literal values must resolve to a single Spark type",
    ):
        type_inspection.get_spark_type_from_python_type(Literal[1, "a"])
