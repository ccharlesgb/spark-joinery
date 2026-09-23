from dataclasses import dataclass
from functools import wraps
import inspect
from typing import (
    Annotated,
    Any,
    Callable,
    Generic,
    Literal,
    ParamSpec,
    TypeVar,
    cast,
    get_args,
    get_origin,
    get_type_hints,
    overload,
)

from pyspark.sql import DataFrame, SparkSession

from spark_joinery.utils import get_callable_name

from .dependencies import Context
from .schemas import CoercionMode, Schema


P = ParamSpec("P")
R = TypeVar("R")


@dataclass(frozen=True)
class Contract:
    coercion_mode: Literal[CoercionMode]
    schema: Schema[Any]

    def is_compatible_with(self, other: "Contract") -> bool:
        return self.schema == other.schema


def ProjectCast(schema: type) -> Contract:
    return Contract(coercion_mode="project_cast", schema=Schema(schema))


def ProjectTopLevel(schema: type) -> Contract:
    return Contract(coercion_mode="project_top_level", schema=Schema(schema))


def Project(schema: type) -> Contract:
    return Contract(coercion_mode="project", schema=Schema(schema))


def Strict(schema: type) -> Contract:
    return Contract(coercion_mode="strict", schema=Schema(schema))


def StrictNull(schema: type) -> Contract:
    return Contract(coercion_mode="strict_null", schema=Schema(schema))


def _get_annotated_dataframe_schema(annotation: Any) -> Contract | None:
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
        if isinstance(metadata_value, Contract):
            try:
                return metadata_value
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
    input_contracts: dict[str, Contract]
    output_contract: Contract | None
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
    input_schemas: dict[str, Contract] = {}
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
        input_contracts=input_schemas,
        output_contract=output_schema,
        spark_parameter=_get_spark_parameter(f),
        context_parameters=context_parameters,
    )


def _wrap_transform(
    fn: Callable[P, R],
    spec: TransformSpec,
) -> Callable[P, R]:
    signature = inspect.signature(fn)

    @wraps(fn)
    def wrapper(*args: P.args, **kwds: P.kwargs) -> R:
        bound_arguments = signature.bind(*args, **kwds)
        bound_arguments.apply_defaults()

        for parameter_name, expected_contract in spec.input_contracts.items():
            value = bound_arguments.arguments.get(parameter_name)
            if not isinstance(value, DataFrame):
                raise TypeError(
                    f"Parameter '{parameter_name}' must be a pyspark.sql.DataFrame"
                )

            try:
                bound_arguments.arguments[parameter_name] = (
                    expected_contract.schema.coerce_dataframe(
                        value, expected_contract.coercion_mode
                    )
                )
            except ValueError as e:
                raise ValueError(
                    f"Schema mismatch for parameter '{parameter_name}'"
                ) from e

        result = fn(*bound_arguments.args, **bound_arguments.kwargs)

        if spec.output_contract is not None:
            if not isinstance(result, DataFrame):
                raise TypeError(
                    f"Return value from '{fn.__name__}' must be a pyspark.sql.DataFrame"
                )

            try:
                result = cast(
                    R,
                    spec.output_contract.schema.coerce_dataframe(
                        result, spec.output_contract.coercion_mode
                    ),
                )
            except ValueError as e:
                raise ValueError(f"Return schema mismatch for '{fn.__name__}'") from e

        return result

    return wrapper


class Transform(Generic[P, R]):
    def __init__(self, fn: Callable[P, R]):
        self._fn = fn
        self.__transform_spec__ = _inspect_transform(fn)

    def __call__(self, *args: P.args, **kwds: P.kwargs) -> R:
        return _wrap_transform(self._fn, self.__transform_spec__)(*args, **kwds)

    def get_signature(self) -> inspect.Signature:
        return inspect.signature(self._fn)

    @property
    def default_name(self) -> str | None:
        return get_callable_name(self._fn)


@overload
def transform(
    f: Callable[P, R],
) -> Transform[P, R]: ...


@overload
def transform(
    f: None = None,
) -> Callable[[Callable[P, R]], Transform[P, R]]: ...


def transform(
    f: Callable[P, R] | None = None,
) -> Transform[P, R] | Callable[[Callable[P, R]], Transform[P, R]]:
    if f is None:
        return Transform

    return Transform(f)
