from dataclasses import dataclass
import inspect
from typing import Annotated, Any, get_args, get_origin, get_type_hints

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import types

from . import schemas
from .dependencies import Context


def _get_annotated_dataframe_schema_model(annotation: Any) -> type[Any] | None:
    if get_origin(annotation) is not Annotated:
        return None

    annotated_args = get_args(annotation)
    if len(annotated_args) < 2:
        return None

    base_type = annotated_args[0]
    metadata = annotated_args[1:]
    if base_type is not DataFrame:
        return None

    for metadata_value in metadata:
        if isinstance(metadata_value, type) and schemas.is_schema_model(metadata_value):
            return metadata_value

    return None


def _get_context_marker(annotation: Any) -> tuple[type, Context] | None:
    if get_origin(annotation) is not Annotated:
        return None

    annotated_args = get_args(annotation)
    if len(annotated_args) < 2:
        return None

    base_type = annotated_args[0]
    metadata = annotated_args[1:]
    for metadata_value in metadata:
        if isinstance(metadata_value, Context):
            return base_type, metadata_value

    return None


@dataclass(frozen=True)
class TransformSpec:
    input_schemas: dict[str, types.StructType]
    output_schema: types.StructType | None
    spark_parameter: str | None
    context_parameters: dict[str, tuple[type, Context]]


def _get_spark_parameter(f: Any) -> str | None:
    type_hints = get_type_hints(f)
    spark_parameters = [
        parameter_name
        for parameter_name in inspect.signature(f).parameters
        if type_hints.get(parameter_name) is SparkSession
    ]
    if len(spark_parameters) > 1:
        raise TypeError("transform may declare only one SparkSession parameter")
    return spark_parameters[0] if spark_parameters else None


def _inspect_transform(f: Any) -> TransformSpec:
    signature = inspect.signature(f)
    type_hints = get_type_hints(f, include_extras=True)
    input_schemas: dict[str, types.StructType] = {}
    context_parameters: dict[str, tuple[type, Context]] = {}

    for parameter_name in signature.parameters:
        parameter_type = type_hints.get(parameter_name)
        if parameter_type is None:
            continue

        dataframe_schema_model = _get_annotated_dataframe_schema_model(parameter_type)
        if dataframe_schema_model is not None:
            input_schemas[parameter_name] = schemas.get_spark_schema_from_model(
                dataframe_schema_model
            )
            continue

        context_marker = _get_context_marker(parameter_type)
        if context_marker is not None:
            context_parameters[parameter_name] = context_marker

    output_schema = None
    return_type = type_hints.get("return")
    if return_type is not None:
        dataframe_schema_model = _get_annotated_dataframe_schema_model(return_type)
        if dataframe_schema_model is not None:
            output_schema = schemas.get_spark_schema_from_model(dataframe_schema_model)

    return TransformSpec(
        input_schemas=input_schemas,
        output_schema=output_schema,
        spark_parameter=_get_spark_parameter(f),
        context_parameters=context_parameters,
    )
