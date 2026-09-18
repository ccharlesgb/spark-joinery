from dataclasses import fields, is_dataclass
from functools import partial
from typing import Any, Callable, TypeVar, Literal

from pyspark.errors import PySparkAssertionError
from pyspark.sql import Column, DataFrame, functions as F, types
from pyspark.testing import assertSchemaEqual

from . import type_inspection

T = TypeVar("T")

CoercionMode = Literal["coerce", "project_all", "project", "strict", "strict_null"]


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


def _coerce_strict(
    dataframe: DataFrame, schema: types.StructType, *, ignore_nullable: bool
) -> DataFrame:
    try:
        assertSchemaEqual(dataframe.schema, schema, ignoreNullable=ignore_nullable)
    except PySparkAssertionError as e:
        raise ValueError("Schema mismatch") from e
    return dataframe


def _build_projected_column(
    column: Column,
    source_type: types.DataType,
    target_type: types.DataType,
    *,
    cast: bool,
    recurse: bool,
) -> Column:
    if recurse and isinstance(target_type, types.StructType):
        if not isinstance(source_type, types.StructType):
            raise ValueError(f"Expected struct type but found {source_type}")

        source_fields = {field.name: field.dataType for field in source_type.fields}
        nested_columns = []
        for target_field in target_type.fields:
            if target_field.name not in source_fields:
                raise ValueError(f"Missing field '{target_field.name}'")
            nested_column = _build_projected_column(
                column.getField(target_field.name),
                source_fields[target_field.name],
                target_field.dataType,
                cast=cast,
                recurse=recurse,
            )
            nested_columns.append(nested_column.alias(target_field.name))
        return F.struct(*nested_columns)

    if recurse and isinstance(target_type, types.ArrayType):
        if not isinstance(source_type, types.ArrayType):
            raise ValueError(f"Expected array type but found {source_type}")

        return F.transform(
            column,
            lambda element: _build_projected_column(
                element,
                source_type.elementType,
                target_type.elementType,
                cast=cast,
                recurse=recurse,
            ),
        )

    if source_type == target_type:
        return column

    if cast:
        return column.cast(target_type)

    raise ValueError(f"Type mismatch: expected {target_type}, found {source_type}")


def _project_fields(
    dataframe: DataFrame, schema: types.StructType, *, cast: bool, recurse: bool
) -> DataFrame:
    source_fields = {field.name: field.dataType for field in dataframe.schema.fields}

    missing_fields = [
        field.name for field in schema.fields if field.name not in source_fields
    ]
    if missing_fields:
        raise ValueError(f"Missing columns: {missing_fields}")

    columns = [
        _build_projected_column(
            F.col(field.name),
            source_fields[field.name],
            field.dataType,
            cast=cast,
            recurse=recurse,
        ).alias(field.name)
        for field in schema.fields
    ]
    return dataframe.select(*columns)


_MODE_HANDLERS: dict[
    CoercionMode, Callable[[DataFrame, types.StructType], DataFrame]
] = {
    "strict": partial(_coerce_strict, ignore_nullable=True),
    "strict_null": partial(_coerce_strict, ignore_nullable=False),
    "project": partial(_project_fields, cast=False, recurse=False),
    "project_all": partial(_project_fields, cast=False, recurse=True),
    "coerce": partial(_project_fields, cast=True, recurse=True),
}


def coerce_dataframe(
    dataframe: DataFrame, schema: types.StructType, mode: CoercionMode
) -> DataFrame:
    return _MODE_HANDLERS[mode](dataframe, schema)
