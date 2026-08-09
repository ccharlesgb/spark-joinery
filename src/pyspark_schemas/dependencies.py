from typing import Any, Sequence


class Context:
    """Marks a transform parameter as resolved from a PipelineContext."""

    def __init__(self, type_: type | None = None) -> None:
        self.type = type_


class PipelineContext:
    """Registry of dependency values, looked up by type."""

    def __init__(self, *, values: Sequence[Any] = ()) -> None:
        self._by_type: dict[type, Any] = {}
        for value in values:
            value_type = type(value)
            if value_type in self._by_type:
                raise ValueError(
                    f"duplicate dependency type {value_type!r} in PipelineContext"
                )
            self._by_type[value_type] = value

    def resolve(self, param_type: type, marker: "Context") -> Any:
        lookup_type = marker.type or param_type
        if lookup_type not in self._by_type:
            raise KeyError(f"no dependency registered for type {lookup_type!r}")
        return self._by_type[lookup_type]
