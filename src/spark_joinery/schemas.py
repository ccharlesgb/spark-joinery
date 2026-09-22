from dataclasses import dataclass, fields, is_dataclass
from collections.abc import Sequence
from typing import Any, Callable, Literal, Protocol, TypeVar

from pyspark.sql import Column, DataFrame, SparkSession, functions as F, types

from spark_joinery.utils import pretty_struct_type

from . import type_inspection

T = TypeVar("T")

CoercionMode = Literal[
    "project_cast", "project", "project_top_level", "strict", "strict_null"
]

DifferenceKind = Literal["additional", "missing", "type_mismatch", "nullable_mismatch"]


@dataclass(frozen=True)
class SchemaDifference:
    kind: DifferenceKind
    path: str
    given: types.DataType | None
    expected: types.DataType | None
    given_nullable: bool | None
    expected_nullable: bool | None


@dataclass(frozen=True)
class SchemaDiff:
    additional: tuple[SchemaDifference, ...]
    missing: tuple[SchemaDifference, ...]
    type_mismatches: tuple[SchemaDifference, ...]
    nullable_mismatches: tuple[SchemaDifference, ...]


def _schema_diff(given: types.StructType, expected: types.StructType) -> SchemaDiff:
    additional: list[SchemaDifference] = []
    missing: list[SchemaDifference] = []
    type_mismatches: list[SchemaDifference] = []
    nullable_mismatches: list[SchemaDifference] = []

    def add_difference(
        kind: DifferenceKind,
        path: str,
        given_type: types.DataType | None,
        expected_type: types.DataType | None,
        given_nullable: bool | None,
        expected_nullable: bool | None,
    ) -> None:
        difference = SchemaDifference(
            kind,
            path,
            given_type,
            expected_type,
            given_nullable,
            expected_nullable,
        )
        match kind:
            case "additional":
                additional.append(difference)
            case "missing":
                missing.append(difference)
            case "type_mismatch":
                type_mismatches.append(difference)
            case "nullable_mismatch":
                nullable_mismatches.append(difference)

    def walk_types(
        given_type: types.DataType,
        expected_type: types.DataType,
        path: str,
        given_nullable: bool | None,
        expected_nullable: bool | None,
    ) -> None:
        if given_nullable != expected_nullable:
            add_difference(
                "nullable_mismatch",
                path,
                given_type,
                expected_type,
                given_nullable,
                expected_nullable,
            )

        if isinstance(given_type, types.StructType) and isinstance(
            expected_type, types.StructType
        ):
            walk_structs(given_type, expected_type, path)
            return

        if isinstance(given_type, types.ArrayType) and isinstance(
            expected_type, types.ArrayType
        ):
            walk_types(
                given_type.elementType,
                expected_type.elementType,
                f"{path}[]",
                given_type.containsNull,
                expected_type.containsNull,
            )
            return

        if given_type != expected_type:
            add_difference(
                "type_mismatch",
                path,
                given_type,
                expected_type,
                given_nullable,
                expected_nullable,
            )

    def walk_structs(
        given_struct: types.StructType,
        expected_struct: types.StructType,
        parent_path: str,
    ) -> None:
        given_fields = {field.name: field for field in given_struct.fields}
        expected_fields = {field.name: field for field in expected_struct.fields}

        for given_field in given_struct.fields:
            path = (
                given_field.name
                if not parent_path
                else f"{parent_path}.{given_field.name}"
            )
            expected_field = expected_fields.get(given_field.name)
            if expected_field is None:
                add_difference(
                    "additional",
                    path,
                    given_field.dataType,
                    None,
                    given_field.nullable,
                    None,
                )
                continue
            walk_types(
                given_field.dataType,
                expected_field.dataType,
                path,
                given_field.nullable,
                expected_field.nullable,
            )

        for expected_field in expected_struct.fields:
            path = (
                expected_field.name
                if not parent_path
                else f"{parent_path}.{expected_field.name}"
            )
            if expected_field.name in given_fields:
                continue
            add_difference(
                "missing",
                path,
                None,
                expected_field.dataType,
                None,
                expected_field.nullable,
            )

    walk_structs(given, expected, "")
    return SchemaDiff(
        tuple(additional),
        tuple(missing),
        tuple(type_mismatches),
        tuple(nullable_mismatches),
    )


_NUMERIC = types.NumericType()
_STRING = types.StringType()
_DATE = types.DateType()
_TIME = types.TimeType()
_TIMESTAMP = types.TimestampType()
_TIMESTAMP_NTZ = types.TimestampNTZType()
_INTERVAL = types.DayTimeIntervalType()
_BOOLEAN = types.BooleanType()
_BINARY = types.BinaryType()
_ARRAY = types.ArrayType(types.NullType())
_MAP = types.MapType(types.NullType(), types.NullType())
_STRUCT = types.StructType()

CAST_MATRIX: dict[tuple[types.DataType, types.DataType], bool] = {
    (_NUMERIC, _NUMERIC): True,
    (_NUMERIC, _STRING): True,
    (_NUMERIC, _DATE): False,
    (_NUMERIC, _TIME): False,
    (_NUMERIC, _TIMESTAMP): True,
    (_NUMERIC, _TIMESTAMP_NTZ): False,
    (_NUMERIC, _INTERVAL): True,
    (_NUMERIC, _BOOLEAN): True,
    (_NUMERIC, _BINARY): False,
    (_NUMERIC, _ARRAY): False,
    (_NUMERIC, _MAP): False,
    (_NUMERIC, _STRUCT): False,
    (_STRING, _NUMERIC): True,
    (_STRING, _STRING): True,
    (_STRING, _DATE): True,
    (_STRING, _TIME): True,
    (_STRING, _TIMESTAMP): True,
    (_STRING, _TIMESTAMP_NTZ): True,
    (_STRING, _INTERVAL): True,
    (_STRING, _BOOLEAN): True,
    (_STRING, _BINARY): True,
    (_STRING, _ARRAY): False,
    (_STRING, _MAP): False,
    (_STRING, _STRUCT): False,
    (_DATE, _NUMERIC): False,
    (_DATE, _STRING): True,
    (_DATE, _DATE): True,
    (_DATE, _TIME): False,
    (_DATE, _TIMESTAMP): True,
    (_DATE, _TIMESTAMP_NTZ): True,
    (_DATE, _INTERVAL): False,
    (_DATE, _BOOLEAN): False,
    (_DATE, _BINARY): False,
    (_DATE, _ARRAY): False,
    (_DATE, _MAP): False,
    (_DATE, _STRUCT): False,
    (_TIME, _NUMERIC): False,
    (_TIME, _STRING): True,
    (_TIME, _DATE): False,
    (_TIME, _TIME): True,
    (_TIME, _TIMESTAMP): False,
    (_TIME, _TIMESTAMP_NTZ): False,
    (_TIME, _INTERVAL): False,
    (_TIME, _BOOLEAN): False,
    (_TIME, _BINARY): False,
    (_TIME, _ARRAY): False,
    (_TIME, _MAP): False,
    (_TIME, _STRUCT): False,
    (_TIMESTAMP, _NUMERIC): True,
    (_TIMESTAMP, _STRING): True,
    (_TIMESTAMP, _DATE): True,
    (_TIMESTAMP, _TIME): False,
    (_TIMESTAMP, _TIMESTAMP): True,
    (_TIMESTAMP, _TIMESTAMP_NTZ): True,
    (_TIMESTAMP, _INTERVAL): False,
    (_TIMESTAMP, _BOOLEAN): False,
    (_TIMESTAMP, _BINARY): False,
    (_TIMESTAMP, _ARRAY): False,
    (_TIMESTAMP, _MAP): False,
    (_TIMESTAMP, _STRUCT): False,
    (_TIMESTAMP_NTZ, _NUMERIC): False,
    (_TIMESTAMP_NTZ, _STRING): True,
    (_TIMESTAMP_NTZ, _DATE): True,
    (_TIMESTAMP_NTZ, _TIME): False,
    (_TIMESTAMP_NTZ, _TIMESTAMP): True,
    (_TIMESTAMP_NTZ, _TIMESTAMP_NTZ): True,
    (_TIMESTAMP_NTZ, _INTERVAL): False,
    (_TIMESTAMP_NTZ, _BOOLEAN): False,
    (_TIMESTAMP_NTZ, _BINARY): False,
    (_TIMESTAMP_NTZ, _ARRAY): False,
    (_TIMESTAMP_NTZ, _MAP): False,
    (_TIMESTAMP_NTZ, _STRUCT): False,
    (_INTERVAL, _NUMERIC): True,
    (_INTERVAL, _STRING): True,
    (_INTERVAL, _DATE): False,
    (_INTERVAL, _TIME): False,
    (_INTERVAL, _TIMESTAMP): False,
    (_INTERVAL, _TIMESTAMP_NTZ): False,
    (_INTERVAL, _INTERVAL): True,
    (_INTERVAL, _BOOLEAN): False,
    (_INTERVAL, _BINARY): False,
    (_INTERVAL, _ARRAY): False,
    (_INTERVAL, _MAP): False,
    (_INTERVAL, _STRUCT): False,
    (_BOOLEAN, _NUMERIC): True,
    (_BOOLEAN, _STRING): True,
    (_BOOLEAN, _DATE): False,
    (_BOOLEAN, _TIME): False,
    (_BOOLEAN, _TIMESTAMP): False,
    (_BOOLEAN, _TIMESTAMP_NTZ): False,
    (_BOOLEAN, _INTERVAL): False,
    (_BOOLEAN, _BOOLEAN): True,
    (_BOOLEAN, _BINARY): False,
    (_BOOLEAN, _ARRAY): False,
    (_BOOLEAN, _MAP): False,
    (_BOOLEAN, _STRUCT): False,
    (_BINARY, _NUMERIC): False,
    (_BINARY, _STRING): True,
    (_BINARY, _DATE): False,
    (_BINARY, _TIME): False,
    (_BINARY, _TIMESTAMP): False,
    (_BINARY, _TIMESTAMP_NTZ): False,
    (_BINARY, _INTERVAL): False,
    (_BINARY, _BOOLEAN): False,
    (_BINARY, _BINARY): True,
    (_BINARY, _ARRAY): False,
    (_BINARY, _MAP): False,
    (_BINARY, _STRUCT): False,
    (_ARRAY, _NUMERIC): False,
    (_ARRAY, _STRING): True,
    (_ARRAY, _DATE): False,
    (_ARRAY, _TIME): False,
    (_ARRAY, _TIMESTAMP): False,
    (_ARRAY, _TIMESTAMP_NTZ): False,
    (_ARRAY, _INTERVAL): False,
    (_ARRAY, _BOOLEAN): False,
    (_ARRAY, _BINARY): False,
    (_ARRAY, _ARRAY): True,
    (_ARRAY, _MAP): False,
    (_ARRAY, _STRUCT): False,
    (_MAP, _NUMERIC): False,
    (_MAP, _STRING): True,
    (_MAP, _DATE): False,
    (_MAP, _TIME): False,
    (_MAP, _TIMESTAMP): False,
    (_MAP, _TIMESTAMP_NTZ): False,
    (_MAP, _INTERVAL): False,
    (_MAP, _BOOLEAN): False,
    (_MAP, _BINARY): False,
    (_MAP, _ARRAY): False,
    (_MAP, _MAP): True,
    (_MAP, _STRUCT): False,
    (_STRUCT, _NUMERIC): False,
    (_STRUCT, _STRING): True,
    (_STRUCT, _DATE): False,
    (_STRUCT, _TIME): False,
    (_STRUCT, _TIMESTAMP): False,
    (_STRUCT, _TIMESTAMP_NTZ): False,
    (_STRUCT, _INTERVAL): False,
    (_STRUCT, _BOOLEAN): False,
    (_STRUCT, _BINARY): False,
    (_STRUCT, _ARRAY): False,
    (_STRUCT, _MAP): False,
    (_STRUCT, _STRUCT): True,
}


def _cast_matrix_type(data_type: types.DataType) -> types.DataType | None:
    if isinstance(data_type, types.NumericType):
        return _NUMERIC
    if isinstance(data_type, types.ArrayType):
        return _ARRAY
    if isinstance(data_type, types.MapType):
        return _MAP
    if isinstance(data_type, types.StructType):
        return _STRUCT
    if isinstance(data_type, types.StringType):
        return _STRING
    if isinstance(data_type, types.DateType):
        return _DATE
    if isinstance(data_type, types.TimeType):
        return _TIME
    if isinstance(data_type, types.TimestampNTZType):
        return _TIMESTAMP_NTZ
    if isinstance(data_type, types.TimestampType):
        return _TIMESTAMP
    if isinstance(data_type, (types.YearMonthIntervalType, types.DayTimeIntervalType)):
        return _INTERVAL
    if isinstance(data_type, types.BooleanType):
        return _BOOLEAN
    if isinstance(data_type, types.BinaryType):
        return _BINARY
    return None


def _is_cast_compatible(
    given: types.DataType | None, expected: types.DataType | None
) -> bool:
    if given is None or expected is None:
        return False
    if given == expected:
        return True

    given_category = _cast_matrix_type(given)
    expected_category = _cast_matrix_type(expected)
    if given_category is None or expected_category is None:
        return False
    return CAST_MATRIX.get((given_category, expected_category), False)


def _format_schema_difference(difference: SchemaDifference) -> str:
    if difference.kind == "missing":
        return f"- {difference.path} (expected {difference.expected})"
    if difference.kind == "additional":
        return f"- {difference.path} (given {difference.given})"
    if difference.kind == "type_mismatch":
        return f"- {difference.path}: {difference.given} -> {difference.expected}"
    return (
        f"- {difference.path}: {difference.given_nullable}"
        f" -> {difference.expected_nullable}"
    )


def _format_schema_coercion_error(
    mode: CoercionMode, violations: tuple[SchemaDifference, ...]
) -> str:
    groups: tuple[tuple[DifferenceKind, str], ...] = (
        ("missing", "Missing fields"),
        ("additional", "Additional fields"),
        (
            "type_mismatch",
            "Unsupported type casts" if mode == "project_cast" else "Type mismatches",
        ),
        ("nullable_mismatch", "Nullable mismatches"),
    )
    lines = [f"Cannot coerce dataframe using mode '{mode}':"]
    for kind, heading in groups:
        differences = [
            difference for difference in violations if difference.kind == kind
        ]
        if not differences:
            continue
        lines.append(f"  {heading}:")
        lines.extend(
            f"    {_format_schema_difference(difference)}" for difference in differences
        )
    return "\n".join(lines)


class SchemaCoercionError(Exception):
    def __init__(
        self, mode: CoercionMode, violations: tuple[SchemaDifference, ...]
    ) -> None:
        self.mode = mode
        self.violations = violations
        super().__init__(_format_schema_coercion_error(mode, violations))


def is_pydantic_model(klass: Any) -> bool:
    # Avoid hard dependency on pydantic; detect by BaseModel class attributes.
    return (
        isinstance(klass, type)
        and hasattr(klass, "model_fields")
        and isinstance(getattr(klass, "model_fields"), dict)
    )


def is_schema_model(klass: Any) -> bool:
    return is_dataclass(klass) or is_pydantic_model(klass)


def _get_model_fields(klass: type[Any]) -> list[tuple[str, Any]]:
    if is_dataclass(klass):
        return [(field.name, field.type) for field in fields(klass)]

    if is_pydantic_model(klass):
        model_fields = getattr(klass, "model_fields")
        return [
            (name, field_info.annotation)
            for name, field_info in model_fields.items()
            if field_info.annotation is not None
        ]

    raise ValueError(f"{klass.__name__} is neither a dataclass nor a pydantic model")


def _get_spark_schema_from_model(klass: type[T]) -> types.StructType:
    if not is_schema_model(klass):
        raise ValueError(
            f"{klass.__name__} is neither a dataclass nor a pydantic model"
        )

    struct_fields = [
        types.StructField(name, _get_spark_field_type(field_type), True)
        for name, field_type in _get_model_fields(klass)
    ]

    return types.StructType(struct_fields)


def _get_spark_field_type(field_type: Any) -> types.DataType:
    annotated_spark_type = type_inspection.spark_type_from_annotated(field_type)
    if annotated_spark_type is not None:
        return annotated_spark_type

    normalized_type = type_inspection.normalize_python_type(field_type)

    if is_schema_model(normalized_type):
        return _get_spark_schema_from_model(normalized_type)

    if type_inspection.is_list(normalized_type):
        element_type = type_inspection.get_list_element_type(normalized_type)
        element_spark_type = _get_spark_field_type(element_type)
        return types.ArrayType(element_spark_type, True)

    return type_inspection.get_spark_type_from_python_type(normalized_type)


def _coerce_strict(
    dataframe: DataFrame,
    schema: types.StructType,
    *,
    mode: Literal["strict", "strict_null"],
    ignore_nullable: bool,
) -> DataFrame:
    diff = _schema_diff(dataframe.schema, schema)
    violations = (*diff.missing, *diff.additional, *diff.type_mismatches)
    if not ignore_nullable:
        violations += diff.nullable_mismatches
    if violations:
        raise SchemaCoercionError(mode, violations)
    return dataframe


def _build_projected_column(
    column: Column,
    source_type: types.DataType,
    target_type: types.DataType,
    *,
    cast: bool,
    recurse: bool,
) -> Column:
    if recurse and isinstance(target_type, types.StructType):
        if not isinstance(source_type, types.StructType):
            raise ValueError(f"Expected struct type but found {source_type}")

        source_fields = {field.name: field.dataType for field in source_type.fields}
        nested_columns = []
        for target_field in target_type.fields:
            if target_field.name not in source_fields:
                raise ValueError(f"Missing field '{target_field.name}'")
            nested_column = _build_projected_column(
                column.getField(target_field.name),
                source_fields[target_field.name],
                target_field.dataType,
                cast=cast,
                recurse=recurse,
            )
            nested_columns.append(nested_column.alias(target_field.name))
        return F.struct(*nested_columns)

    if recurse and isinstance(target_type, types.ArrayType):
        if not isinstance(source_type, types.ArrayType):
            raise ValueError(f"Expected array type but found {source_type}")

        return F.transform(
            column,
            lambda element: _build_projected_column(
                element,
                source_type.elementType,
                target_type.elementType,
                cast=cast,
                recurse=recurse,
            ),
        )

    if source_type == target_type:
        return column

    if cast:
        return column.cast(target_type)

    raise ValueError(f"Type mismatch: expected {target_type}, found {source_type}")


def _project_fields(
    dataframe: DataFrame,
    schema: types.StructType,
    *,
    mode: CoercionMode,
    cast: bool,
    recurse: bool,
) -> DataFrame:
    diff = _schema_diff(dataframe.schema, schema)
    type_mismatches = (
        tuple(
            difference
            for difference in diff.type_mismatches
            if not _is_cast_compatible(difference.given, difference.expected)
        )
        if cast
        else diff.type_mismatches
    )
    violations = (*diff.missing, *type_mismatches)
    if violations:
        raise SchemaCoercionError(mode, violations)

    source_fields = {field.name: field.dataType for field in dataframe.schema.fields}

    columns = [
        _build_projected_column(
            F.col(field.name),
            source_fields[field.name],
            field.dataType,
            cast=cast,
            recurse=recurse,
        ).alias(field.name)
        for field in schema.fields
    ]
    return dataframe.select(*columns)


class SchemaCoercionMode(Protocol):
    def __call__(self, dataframe: DataFrame, schema: types.StructType) -> DataFrame: ...


class Strict(SchemaCoercionMode):
    def __call__(self, dataframe: DataFrame, schema: types.StructType) -> DataFrame:
        return _coerce_strict(dataframe, schema, mode="strict", ignore_nullable=True)


class StrictNull(SchemaCoercionMode):
    def __call__(self, dataframe: DataFrame, schema: types.StructType) -> DataFrame:
        return _coerce_strict(
            dataframe, schema, mode="strict_null", ignore_nullable=False
        )


class ProjectTopLevel(SchemaCoercionMode):
    def __call__(self, dataframe: DataFrame, schema: types.StructType) -> DataFrame:
        return _project_fields(
            dataframe, schema, mode="project", cast=False, recurse=False
        )


class Project(SchemaCoercionMode):
    def __call__(self, dataframe: DataFrame, schema: types.StructType) -> DataFrame:
        return _project_fields(
            dataframe, schema, mode="project", cast=False, recurse=True
        )


class ProjectCast(SchemaCoercionMode):
    def __call__(self, dataframe: DataFrame, schema: types.StructType) -> DataFrame:
        return _project_fields(
            dataframe, schema, mode="project_cast", cast=True, recurse=True
        )


_MODE_HANDLERS: dict[
    CoercionMode, Callable[[DataFrame, types.StructType], DataFrame]
] = {
    "strict": Strict(),
    "strict_null": StrictNull(),
    "project": Project(),
    "project_top_level": ProjectTopLevel(),
    "project_cast": ProjectCast(),
}


class Schema[T]:
    def __init__(self, model: type[T]):
        if not is_schema_model(model):
            raise ValueError(
                f"{model.__name__} is neither a dataclass nor a pydantic model"
            )
        self.model = model
        self._spark_schema = _get_spark_schema_from_model(self.model)

    @property
    def spark_schema(self) -> types.StructType:
        return self._spark_schema

    def create_dataframe(self, spark: SparkSession, rows: Sequence[T]) -> DataFrame:
        for index, row in enumerate(rows):
            if not isinstance(row, self.model):
                raise ValueError(
                    f"Row {index} of type {row.__class__.__name__}. Expected type {self.model.__name__}"
                )

        serialized_rows = [
            model_dump(mode="python")
            if callable(model_dump := getattr(row, "model_dump", None))
            else row
            for row in rows
        ]
        return spark.createDataFrame(serialized_rows, self.spark_schema)

    def coerce_dataframe(
        self, dataframe: DataFrame, mode: CoercionMode = "project"
    ) -> DataFrame:
        try:
            handler = _MODE_HANDLERS[mode]
            return handler(dataframe, self.spark_schema)
        except KeyError as e:
            raise ValueError(f"Unsupported coercion mode: {mode}") from e

    def __repr__(self) -> str:
        return f"Schema(model={self.model.__name__})"

    @property
    def pretty_schema(self) -> str:
        return pretty_struct_type(self.spark_schema)
