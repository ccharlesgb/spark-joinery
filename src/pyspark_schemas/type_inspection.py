import datetime
import decimal
from types import UnionType

from pyspark.sql import types
from typing import Any, Annotated, Literal, get_origin, get_args, Union

NoneType = type(None)


SPARK_MAX_DECIMAL_PRECISION = 38
DEFAULT_FRACTIONAL_DIGITS = 18


def allows_none(tp: Any) -> bool:
    origin = get_origin(tp)
    args = get_args(tp)

    # Optional[T] is Union[T, None]
    if origin is Union and NoneType in args:
        return True

    # PEP 604 syntax: T | None
    if origin is UnionType and NoneType in args:
        return True

    return False


def is_list(tp: Any) -> bool:
    tp = normalize_python_type(tp)
    origin = get_origin(tp)
    return origin is list


def get_list_element_type(tp: Any) -> Any:
    tp = normalize_python_type(tp)
    if not is_list(tp):
        raise ValueError(f"{tp} is not a list type")
    args = get_args(tp)
    if len(args) != 1:
        raise ValueError(f"List type {tp} should have exactly one type argument")
    return args[0]


def normalize_python_type(python_type: Any) -> Any:
    origin = get_origin(python_type)
    args = get_args(python_type)

    if origin is Annotated and args:
        return normalize_python_type(args[0])

    if origin in (Union, UnionType) and args:
        non_none_args = [arg for arg in args if arg is not NoneType]
        if len(non_none_args) == 1 and len(non_none_args) != len(args):
            return normalize_python_type(non_none_args[0])

    return python_type


def spark_type_from_annotated(python_type: Any) -> types.DataType | None:
    origin = get_origin(python_type)
    args = get_args(python_type)

    if origin is not Annotated or len(args) < 2:
        return None

    for metadata in args[1:]:
        if isinstance(metadata, types.DataType):
            return metadata

    return None


def spark_type_from_literal(python_type: Any) -> types.DataType | None:
    if get_origin(python_type) is not Literal:
        return None

    literal_values = get_args(python_type)
    if not literal_values:
        raise ValueError("Literal type must include at least one value")

    spark_types = {
        get_spark_type_from_python_type(type(literal_value))
        for literal_value in literal_values
    }

    if len(spark_types) != 1:
        raise ValueError(
            f"Literal values must resolve to a single Spark type: {python_type}"
        )

    return next(iter(spark_types))


def get_spark_type_from_python_type(python_type: type) -> types.DataType:
    annotated_spark_type = spark_type_from_annotated(python_type)
    if annotated_spark_type is not None:
        return annotated_spark_type

    literal_spark_type = spark_type_from_literal(python_type)
    if literal_spark_type is not None:
        return literal_spark_type

    python_type = normalize_python_type(python_type)

    if python_type is int:
        return types.IntegerType()
    elif python_type is str:
        return types.StringType()
    elif python_type is float:
        return types.FloatType()
    elif python_type is bool:
        return types.BooleanType()
    elif python_type is datetime.datetime:
        return types.TimestampType()
    elif python_type is datetime.date:
        return types.DateType()
    elif python_type is decimal.Decimal:
        return types.DecimalType(SPARK_MAX_DECIMAL_PRECISION, DEFAULT_FRACTIONAL_DIGITS)
    elif python_type is bytes:
        return types.BinaryType()
    else:
        raise ValueError(f"Unsupported Python type: {python_type}")
