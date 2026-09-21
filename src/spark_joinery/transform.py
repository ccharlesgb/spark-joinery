from dataclasses import dataclass
from functools import wraps
import inspect
from typing import (
    Annotated,
    Any,
    Callable,
    ParamSpec,
    TypeVar,
    cast,
    get_args,
    get_origin,
    get_type_hints,
    overload,
)

from pyspark.sql import DataFrame, SparkSession

from .dependencies import Context
from .schemas import CoercionMode, Schema

P = ParamSpec("P")
R = TypeVar("R")


def _get_annotated_dataframe_schema(annotation: Any) -> Schema[Any] | None:
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
        if isinstance(metadata_value, type):
            try:
                return Schema(metadata_value)
            except ValueError:
                continue

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
    input_schemas: dict[str, Schema[Any]]
    output_schema: Schema[Any] | None
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
    input_schemas: dict[str, Schema[Any]] = {}
    context_parameters: dict[str, tuple[type, Context]] = {}

    for parameter_name in signature.parameters:
        parameter_type = type_hints.get(parameter_name)
        if parameter_type is None:
            continue

        dataframe_schema = _get_annotated_dataframe_schema(parameter_type)
        if dataframe_schema is not None:
            input_schemas[parameter_name] = dataframe_schema
            continue

        context_marker = _get_context_marker(parameter_type)
        if context_marker is not None:
            context_parameters[parameter_name] = context_marker

    output_schema = None
    return_type = type_hints.get("return")
    if return_type is not None:
        dataframe_schema = _get_annotated_dataframe_schema(return_type)
        if dataframe_schema is not None:
            output_schema = dataframe_schema

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
    validate_input: CoercionMode | None,
    validate_output: CoercionMode | None,
) -> Callable[P, R]:
    signature = inspect.signature(fn)

    @wraps(fn)
    def wrapper(*args: P.args, **kwds: P.kwargs) -> R:
        bound_arguments = signature.bind(*args, **kwds)
        bound_arguments.apply_defaults()

        if validate_input is not None:
            for parameter_name, expected_schema in spec.input_schemas.items():
                value = bound_arguments.arguments.get(parameter_name)
                if not isinstance(value, DataFrame):
                    raise TypeError(
                        f"Parameter '{parameter_name}' must be a pyspark.sql.DataFrame"
                    )

                try:
                    bound_arguments.arguments[parameter_name] = (
                        expected_schema.coerce_dataframe(value, validate_input)
                    )
                except ValueError as e:
                    raise ValueError(
                        f"Schema mismatch for parameter '{parameter_name}'"
                    ) from e

        result = fn(*bound_arguments.args, **bound_arguments.kwargs)

        if validate_output is not None and spec.output_schema is not None:
            if not isinstance(result, DataFrame):
                raise TypeError(
                    f"Return value from '{fn.__name__}' must be a pyspark.sql.DataFrame"
                )

            try:
                result = cast(
                    R,
                    spec.output_schema.coerce_dataframe(result, validate_output),
                )
            except ValueError as e:
                raise ValueError(f"Return schema mismatch for '{fn.__name__}'") from e

        return result

    return wrapper


@overload
def transform(
    f: Callable[P, R],
    *,
    validate_input: CoercionMode | None = "project_all",
    validate_output: CoercionMode | None = "project_all",
) -> Callable[P, R]: ...


@overload
def transform(
    f: None = None,
    *,
    validate_input: CoercionMode | None = "project_all",
    validate_output: CoercionMode | None = "project_all",
) -> Callable[[Callable[P, R]], Callable[P, R]]: ...


def transform(
    f: Callable[P, R] | None = None,
    *,
    validate_input: CoercionMode | None = "project_all",
    validate_output: CoercionMode | None = "project_all",
):
    def decorator(fn: Callable[P, R]) -> Callable[P, R]:
        spec = _inspect_transform(fn)
        return _wrap_transform(
            fn,
            spec,
            validate_input=validate_input,
            validate_output=validate_output,
        )

    if f is None:
        return decorator

    return decorator(f)
