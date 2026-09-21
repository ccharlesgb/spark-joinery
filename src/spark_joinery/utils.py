from typing import Callable

from pyspark.sql import types


_INDENT = "    "


def get_callable_name(callable: Callable) -> str | None:
    name_attr: str | None = None
    if hasattr(callable, "__name__"):
        name_attr = callable.__name__
        if name_attr == "<lambda>":
            name_attr = None
    elif hasattr(callable, "__class__"):
        name_attr = callable.__class__.__name__

    return name_attr


def pretty_print_struct_type(struct_type: types.StructType) -> None:
    print(_format_struct_type(struct_type, indentation=0))


def pretty_struct_type(struct_type: types.StructType) -> str:
    return _format_struct_type(struct_type, indentation=0)


def _format_struct_type(struct_type: types.StructType, indentation: int) -> str:
    indent = _INDENT * indentation
    fields = [
        f"{_INDENT * (indentation + 1)}{_format_struct_field(field, indentation + 1)}"
        for field in struct_type.fields
    ]
    return f"StructType([\n{',\n'.join(fields)}\n{indent}])"


def _format_struct_field(field: types.StructField, indentation: int) -> str:
    data_type = _format_data_type(field.dataType, indentation + 1)
    if "\n" not in data_type:
        return f"StructField({field.name!r}, {data_type}, {field.nullable!r})"

    indent = _INDENT * indentation
    nested_indent = _INDENT * (indentation + 1)
    return (
        f"StructField(\n{nested_indent}{field.name!r},\n{nested_indent}{data_type},\n"
        f"{nested_indent}{field.nullable!r}\n{indent})"
    )


def _format_data_type(data_type: types.DataType, indentation: int) -> str:
    if isinstance(data_type, types.StructType):
        return _format_struct_type(data_type, indentation)

    if isinstance(data_type, types.ArrayType):
        element_type = _format_data_type(data_type.elementType, indentation + 1)
        if "\n" in element_type:
            indent = _INDENT * indentation
            nested_indent = _INDENT * (indentation + 1)
            return (
                f"ArrayType(\n{nested_indent}{element_type},\n"
                f"{nested_indent}{data_type.containsNull!r}\n{indent})"
            )

    return repr(data_type)
