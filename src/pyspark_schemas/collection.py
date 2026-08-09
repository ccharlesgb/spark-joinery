import inspect
from functools import wraps
from typing import Any, Callable, ParamSpec, TypeVar, overload

from pyspark.errors import PySparkAssertionError
from pyspark.sql import DataFrame
from pyspark.testing import assertSchemaEqual

from .transform import TransformSpec, _inspect_transform

P = ParamSpec("P")
R = TypeVar("R")


class Collection:
    def __init__(self) -> None:
        self._specs: dict[Callable[..., Any], TransformSpec] = {}

    @overload
    def transform(
        self,
        f: Callable[P, R],
        *,
        validate_input: bool = True,
        validate_output: bool = True,
        ignore_nullable: bool = True,
    ) -> Callable[P, R]: ...

    @overload
    def transform(
        self,
        f: None = None,
        *,
        validate_input: bool = True,
        validate_output: bool = True,
        ignore_nullable: bool = True,
    ) -> Callable[[Callable[P, R]], Callable[P, R]]: ...

    def transform(
        self,
        f: Callable[P, R] | None = None,
        *,
        validate_input: bool = True,
        validate_output: bool = True,
        ignore_nullable: bool = True,
    ):
        def decorator(fn: Callable[P, R]) -> Callable[P, R]:
            signature = inspect.signature(fn)
            spec = _inspect_transform(fn)

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
                        raise ValueError(
                            f"Return schema mismatch for '{fn.__name__}'"
                        ) from e

                return result

            self._specs[wrapper] = spec
            return wrapper

        if f is None:
            return decorator

        return decorator(f)
