from dataclasses import fields, is_dataclass
from typing import Any, TypeVar

from pyspark.sql import DataFrame, types

from . import type_inspection

T = TypeVar("T")


def is_pydantic_model(klass: Any) -> bool:
    # Avoid hard dependency on pydantic; detect by BaseModel class attributes.
    return (
        isinstance(klass, type)
        and hasattr(klass, "model_fields")
        and isinstance(getattr(klass, "model_fields"), dict)
    )


def is_schema_model(klass: Any) -> bool:
    return is_dataclass(klass) or is_pydantic_model(klass)


def _get_model_fields(klass: type[Any]) -> list[tuple[str, Any]]:
    if is_dataclass(klass):
        return [(field.name, field.type) for field in fields(klass)]

    if is_pydantic_model(klass):
        model_fields = getattr(klass, "model_fields")
        return [
            (name, field_info.annotation)
            for name, field_info in model_fields.items()
            if field_info.annotation is not None
        ]

    raise ValueError(f"{klass.__name__} is neither a dataclass nor a pydantic model")


def _get_spark_field_type(field_type: Any) -> types.DataType:
    normalized_type = type_inspection.normalize_python_type(field_type)

    if is_schema_model(normalized_type):
        return get_spark_schema_from_model(normalized_type)

    if type_inspection.is_list(normalized_type):
        element_type = type_inspection.get_list_element_type(normalized_type)
        element_spark_type = _get_spark_field_type(element_type)
        return types.ArrayType(element_spark_type, True)

    return type_inspection.get_spark_type_from_python_type(normalized_type)


def get_spark_schema_from_model(klass: type[T]) -> types.StructType:
    if not is_schema_model(klass):
        raise ValueError(
            f"{klass.__name__} is neither a dataclass nor a pydantic model"
        )

    struct_fields = [
        types.StructField(name, _get_spark_field_type(field_type), True)
        for name, field_type in _get_model_fields(klass)
    ]

    return types.StructType(struct_fields)


def get_spark_schema_from_dataclass(klass: type[T]) -> types.StructType:
    if not is_dataclass(klass):
        raise ValueError(f"{klass.__name__} is not a dataclass")

    return get_spark_schema_from_model(klass)


def dataframe_is_schema(dataframe: DataFrame, klass: type[T]) -> bool:
    expected_schema = get_spark_schema_from_dataclass(klass)
    return dataframe.schema == expected_schema


def dataframe_is_model_schema(dataframe: DataFrame, klass: type[T]) -> bool:
    expected_schema = get_spark_schema_from_model(klass)
    return dataframe.schema == expected_schema
