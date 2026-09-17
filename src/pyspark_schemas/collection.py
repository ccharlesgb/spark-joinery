from typing import Any, Callable, ParamSpec, TypeVar, overload

from .transform import TransformSpec, _inspect_transform, _wrap_transform

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
            spec = _inspect_transform(fn)
            wrapper = _wrap_transform(
                fn,
                spec,
                validate_input=validate_input,
                validate_output=validate_output,
                ignore_nullable=ignore_nullable,
            )
            self._specs[wrapper] = spec
            return wrapper

        if f is None:
            return decorator

        return decorator(f)
