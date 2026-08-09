from dataclasses import dataclass
from typing import Annotated, Generator

import pytest
from pyspark.sql import DataFrame, SparkSession, types

from pyspark_schemas import fixtures, schemas
from pyspark_schemas.collection import Collection


@pytest.fixture(scope="session")
def spark() -> Generator[SparkSession, None, None]:
    spark = (
        SparkSession.builder.appName("pyspark-schemas-collection-tests")
        .master("local[*]")
        .getOrCreate()
    )
    yield spark
    spark.stop()


def test_transform_accepts_matching_input_and_output_schemas(spark: SparkSession):
    @dataclass
    class InputRow:
        field1: int
        field2: str

    @dataclass
    class OutputRow:
        field1: int

    input_schema = schemas.get_spark_schema_from_dataclass(InputRow)
    output_schema = schemas.get_spark_schema_from_dataclass(OutputRow)
    input_df = spark.createDataFrame([(1, "a")], input_schema)

    collection = Collection()

    @collection.transform
    def my_function(
        input1: Annotated[DataFrame, InputRow],
    ) -> Annotated[DataFrame, OutputRow]:
        return input1.select("field1")

    result = my_function(input_df)
    assert result.schema == output_schema


def test_transform_raises_for_input_schema_mismatch(spark: SparkSession):
    @dataclass
    class InputRow:
        field1: int
        field2: str

    bad_input_df = spark.createDataFrame(
        [(1,)],
        types.StructType([types.StructField("field1", types.IntegerType(), False)]),
    )

    collection = Collection()

    @collection.transform
    def my_function(input1: Annotated[DataFrame, InputRow]):
        return input1

    with pytest.raises(ValueError, match="Schema mismatch for parameter 'input1'"):
        my_function(bad_input_df)


def test_transform_raises_for_output_schema_mismatch(spark: SparkSession):
    @dataclass
    class InputRow:
        field1: int
        field2: str

    @dataclass
    class OutputRow:
        field1: int

    input_df = fixtures.get_dataframe(spark, InputRow, [InputRow(1, "a")])

    collection = Collection()

    @collection.transform
    def my_function(
        input1: Annotated[DataFrame, InputRow],
    ) -> Annotated[DataFrame, OutputRow]:
        return input1

    with pytest.raises(ValueError, match="Return schema mismatch for 'my_function'"):
        my_function(input_df)


def test_transform_raises_for_non_dataframe_return(spark: SparkSession):
    @dataclass
    class InputRow:
        field1: int

    @dataclass
    class OutputRow:
        field1: int

    input_df = fixtures.get_dataframe(spark, InputRow, [InputRow(1)])

    collection = Collection()

    @collection.transform
    def my_function(
        input1: Annotated[DataFrame, InputRow],
    ) -> Annotated[DataFrame, OutputRow]:
        return "not a dataframe"  # type: ignore

    with pytest.raises(
        TypeError,
        match="Return value from 'my_function' must be a pyspark.sql.DataFrame",
    ):
        my_function(input_df)


def test_transform_parameterized_no_args_still_validates(spark: SparkSession):
    @dataclass
    class InputRow:
        field1: int
        field2: str

    bad_input_df = spark.createDataFrame(
        [(1,)],
        types.StructType([types.StructField("field1", types.IntegerType(), False)]),
    )

    collection = Collection()

    @collection.transform()
    def my_function(input1: Annotated[DataFrame, InputRow]):
        return input1

    with pytest.raises(ValueError, match="Schema mismatch for parameter 'input1'"):
        my_function(bad_input_df)


def test_transform_can_disable_output_validation(spark: SparkSession):
    @dataclass
    class InputRow:
        field1: int

    @dataclass
    class OutputRow:
        field1: int

    input_df = fixtures.get_dataframe(spark, InputRow, [InputRow(1)])

    collection = Collection()

    @collection.transform(validate_output=False)
    def my_function(
        input1: Annotated[DataFrame, InputRow],
    ) -> Annotated[DataFrame, OutputRow]:
        return "not a dataframe"  # type: ignore

    assert my_function(input_df) == "not a dataframe"


def test_transform_accepts_pydantic_input_and_output_schemas(spark: SparkSession):
    from pydantic import BaseModel

    class InputRow(BaseModel):
        field1: int
        field2: str

    class OutputRow(BaseModel):
        field1: int

    input_schema = schemas.get_spark_schema_from_model(InputRow)
    output_schema = schemas.get_spark_schema_from_model(OutputRow)
    input_df = spark.createDataFrame([(1, "a")], input_schema)

    collection = Collection()

    @collection.transform
    def my_function(
        input1: Annotated[DataFrame, InputRow],
    ) -> Annotated[DataFrame, OutputRow]:
        return input1.select("field1")

    result = my_function(input_df)
    assert result.schema == output_schema


def test_collection_stores_spec_without_patching_function():
    @dataclass
    class InputRow:
        field1: int

    @dataclass
    class OutputRow:
        field2: str

    collection = Collection()

    @collection.transform
    def my_function(
        input1: Annotated[DataFrame, InputRow],
    ) -> Annotated[DataFrame, OutputRow]:
        return input1.selectExpr("cast(field1 as string) as field2")

    assert not hasattr(my_function, "__transform_spec__")

    spec = collection._specs[my_function]
    assert spec.input_schemas == {
        "input1": schemas.get_spark_schema_from_dataclass(InputRow)
    }
    assert spec.output_schema == schemas.get_spark_schema_from_dataclass(OutputRow)


def test_collection_metadata_returns_none_for_unannotated_output():
    collection = Collection()

    @collection.transform
    def my_function():
        return "not a dataframe"

    assert collection._specs[my_function].output_schema is None
