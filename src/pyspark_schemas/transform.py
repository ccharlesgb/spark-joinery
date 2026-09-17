from dataclasses import dataclass
from functools import wraps
import inspect
from typing import (
    Annotated,
    Any,
    Callable,
    ParamSpec,
    TypeVar,
    get_args,
    get_origin,
    get_type_hints,
    overload,
)

from pyspark.errors import PySparkAssertionError
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import types
from pyspark.testing import assertSchemaEqual

from . import schemas
from .dependencies import Context

P = ParamSpec("P")
R = TypeVar("R")


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


def _wrap_transform(
    fn: Callable[P, R],
    spec: TransformSpec,
    *,
    validate_input: bool,
    validate_output: bool,
    ignore_nullable: bool,
) -> Callable[P, R]:
    signature = inspect.signature(fn)

    @wraps(fn)
    def wrapper(*args: P.args, **kwds: P.kwargs) -> R:
        bound_arguments = signature.bind(*args, **kwds)
        bound_arguments.apply_defaults()

        if validate_input:
            for parameter_name, expected_schema in spec.input_schemas.items():
                value = bound_arguments.arguments.get(parameter_name)
                if not isinstance(value, DataFrame):
                    raise TypeError(
                        f"Parameter '{parameter_name}' must be a pyspark.sql.DataFrame"
                    )

                try:
                    assertSchemaEqual(
                        value.schema,
                        expected_schema,
                        ignoreNullable=ignore_nullable,
                    )
                except PySparkAssertionError as e:
                    raise ValueError(
                        f"Schema mismatch for parameter '{parameter_name}'"
                    ) from e

        result = fn(*args, **kwds)

        if validate_output and spec.output_schema is not None:
            if not isinstance(result, DataFrame):
                raise TypeError(
                    f"Return value from '{fn.__name__}' must be a pyspark.sql.DataFrame"
                )

            try:
                assertSchemaEqual(
                    result.schema,
                    spec.output_schema,
                    ignoreNullable=ignore_nullable,
                )
            except PySparkAssertionError as e:
                raise ValueError(f"Return schema mismatch for '{fn.__name__}'") from e

        return result

    return wrapper


@overload
def transform(
    f: Callable[P, R],
    *,
    validate_input: bool = True,
    validate_output: bool = True,
    ignore_nullable: bool = True,
) -> Callable[P, R]: ...


@overload
def transform(
    f: None = None,
    *,
    validate_input: bool = True,
    validate_output: bool = True,
    ignore_nullable: bool = True,
) -> Callable[[Callable[P, R]], Callable[P, R]]: ...


def transform(
    f: Callable[P, R] | None = None,
    *,
    validate_input: bool = True,
    validate_output: bool = True,
    ignore_nullable: bool = True,
):
    def decorator(fn: Callable[P, R]) -> Callable[P, R]:
        spec = _inspect_transform(fn)
        return _wrap_transform(
            fn,
            spec,
            validate_input=validate_input,
            validate_output=validate_output,
            ignore_nullable=ignore_nullable,
        )

    if f is None:
        return decorator

    return decorator(f)
