from dataclasses import dataclass
from typing import Annotated, Generator

import pytest
from pyspark.sql import DataFrame, SparkSession, types

from spark_joinery import (
    Schema,
    schemas,
    ProjectCast,
    Strict,
    StrictNull,
    Project,
)
from spark_joinery.dependencies import Context
from spark_joinery.transform import Contract, Transform, _inspect_transform, transform


@dataclass
class Order:
    order_id: int


@dataclass(frozen=True)
class PathConfig:
    value: str


@pytest.fixture(scope="session")
def spark() -> Generator[SparkSession, None, None]:
    spark = (
        SparkSession.builder.appName("pyspark-schemas-transform-tests")
        .master("local[*]")
        .getOrCreate()
    )
    yield spark
    spark.stop()


def test_inspect_transform_collects_context_parameters():
    def read_orders(
        spark: SparkSession, path: Annotated[PathConfig, Context()]
    ) -> Annotated[DataFrame, Order]:
        raise AssertionError

    spec = _inspect_transform(read_orders)

    assert set(spec.context_parameters) == {"path"}
    declared_type, marker = spec.context_parameters["path"]
    assert declared_type is PathConfig
    assert isinstance(marker, Context)


def test_inspect_transform_context_parameter_not_treated_as_dataframe_input():
    def filter_orders(
        orders: Annotated[DataFrame, Project(Order)],
        path: Annotated[PathConfig, Context()],
    ) -> Annotated[DataFrame, Order]:
        raise AssertionError

    spec = _inspect_transform(filter_orders)

    assert set(spec.input_contracts) == {"orders"}
    assert isinstance(spec.input_contracts["orders"], Contract)
    assert spec.input_contracts["orders"].schema.model is Order
    assert set(spec.context_parameters) == {"path"}


def test_inspect_transform_with_no_context_parameters_has_empty_dict():
    def read_orders(spark: SparkSession) -> Annotated[DataFrame, Order]:
        raise AssertionError

    spec = _inspect_transform(read_orders)

    assert spec.context_parameters == {}


def test_transform_accepts_matching_input_and_output_schemas(spark: SparkSession):
    @dataclass
    class InputRow:
        field1: int
        field2: str

    @dataclass
    class OutputRow:
        field1: int

    input_schema = Schema(InputRow).spark_schema
    output_schema = Schema(OutputRow).spark_schema
    input_df = spark.createDataFrame([(1, "a")], input_schema)

    @transform
    def my_function(
        input1: Annotated[DataFrame, InputRow],
    ) -> Annotated[DataFrame, OutputRow]:
        return input1.select("field1")

    result = my_function(input_df)
    assert result.schema == output_schema


def test_transform_retains_inspected_specification():
    @dataclass
    class InputRow:
        field1: int

    @transform
    def my_function(input1: Annotated[DataFrame, Project(InputRow)]):
        return input1

    assert isinstance(my_function, Transform)
    assert (
        my_function.__transform_spec__.input_contracts["input1"].schema.model
        is InputRow
    )


def test_transform_raises_for_input_schema_mismatch(spark: SparkSession):
    @dataclass
    class InputRow:
        field1: int
        field2: str

    bad_input_df = spark.createDataFrame(
        [(1,)],
        types.StructType([types.StructField("field1", types.LongType(), False)]),
    )

    @transform
    def my_function(input1: Annotated[DataFrame, Project(InputRow)]):
        return input1

    with pytest.raises(schemas.SchemaCoercionError) as error:
        my_function(bad_input_df)

    assert [
        (violation.kind, violation.path) for violation in error.value.violations
    ] == [("missing", "field2")]


def test_transform_raises_for_output_schema_mismatch(spark: SparkSession):
    @dataclass
    class InputRow:
        field1: int
        field2: str

    @dataclass
    class OutputRow:
        field1: int

    input_df = Schema(InputRow).create_dataframe(spark, [InputRow(1, "a")])

    @transform
    def my_function(
        input1: Annotated[DataFrame, InputRow],
    ) -> Annotated[DataFrame, Strict(OutputRow)]:
        return input1

    with pytest.raises(schemas.SchemaCoercionError) as error:
        my_function(input_df)

    assert [
        (violation.kind, violation.path) for violation in error.value.violations
    ] == [("additional", "field2")]


def test_transform_can_disable_output_validation(spark: SparkSession):
    @dataclass
    class InputRow:
        field1: int

    @dataclass
    class OutputRow:
        field1: int

    input_df = Schema(InputRow).create_dataframe(spark, [InputRow(1)])

    @transform
    def my_function(
        input1: Annotated[DataFrame, InputRow],
    ) -> Annotated[DataFrame, OutputRow]:
        return "not a dataframe"  # type: ignore

    assert my_function(input_df) == "not a dataframe"


def test_transform_project_drops_extra_output_columns(
    spark: SparkSession,
):
    @dataclass
    class InputRow:
        field1: int
        field2: str

    @dataclass
    class OutputRow:
        field1: int

    input_df = Schema(InputRow).create_dataframe(spark, [InputRow(1, "a")])

    @transform
    def my_function(
        input1: Annotated[DataFrame, Project(InputRow)],
    ) -> Annotated[DataFrame, Project(OutputRow)]:
        return input1

    result = my_function(input_df)
    assert result.columns == ["field1"]


def test_transform_strict_null_raises_for_nullability_mismatch(spark: SparkSession):
    @dataclass
    class InputRow:
        field1: int

    bad_input_df = spark.createDataFrame(
        [(1,)],
        types.StructType([types.StructField("field1", types.LongType(), False)]),
    )

    @transform
    def my_function(input1: Annotated[DataFrame, StrictNull(InputRow)]):
        return input1

    with pytest.raises(schemas.SchemaCoercionError) as error:
        my_function(bad_input_df)

    assert [
        (violation.kind, violation.path) for violation in error.value.violations
    ] == [("nullable_mismatch", "field1")]


def test_transform_project_cast_mode_casts_input_dataframe(spark: SparkSession):
    @dataclass
    class InputRow:
        field1: int

    input_df = spark.createDataFrame(
        [(1.9,)],
        types.StructType([types.StructField("field1", types.DoubleType(), True)]),
    )

    @transform
    def my_function(input1: Annotated[DataFrame, ProjectCast(InputRow)]):
        return input1

    result = my_function(input_df)
    assert result.schema["field1"].dataType == types.LongType()
    assert result.collect() == [types.Row(field1=1)]
