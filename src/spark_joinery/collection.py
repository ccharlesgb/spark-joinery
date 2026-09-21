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
    ) -> Callable[P, R]: ...

    @overload
    def transform(
        self,
        f: None = None,
    ) -> Callable[[Callable[P, R]], Callable[P, R]]: ...

    def transform(
        self,
        f: Callable[P, R] | None = None,
    ):
        def decorator(fn: Callable[P, R]) -> Callable[P, R]:
            spec = _inspect_transform(fn)
            wrapper = _wrap_transform(
                fn,
                spec,
            )
            self._specs[wrapper] = spec
            return wrapper

        if f is None:
            return decorator

        return decorator(f)
