from typing import Callable


def get_callable_name(callable: Callable) -> str | None:
    name_attr: str | None = None
    if hasattr(callable, "__name__"):
        name_attr = callable.__name__
        if name_attr == "<lambda>":
            name_attr = None
    elif hasattr(callable, "__class__"):
        name_attr = callable.__class__.__name__

    return name_attr
