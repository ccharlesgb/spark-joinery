import pytest
from pyspark.sql import SparkSession
from pyspark.testing import assertDataFrameEqual
from typing import Generator
from dataclasses import dataclass
from pydantic import BaseModel
from pyspark_schemas.fixtures import get_dataframe
from pyspark.sql import types


@pytest.fixture(scope="session")
def spark() -> Generator[SparkSession, None, None]:
    spark = (
        SparkSession.builder.appName("pyspark-schemas-tests")
        .master("local[*]")
        .getOrCreate()
    )
    yield spark
    spark.stop()


def test_get_dataframe_makes_spark_dataframe(spark: SparkSession):
    @dataclass
    class MyDataClass:
        field1: int
        field2: str

    rows = [MyDataClass(1, "a"), MyDataClass(2, "b")]
    df = get_dataframe(spark, MyDataClass, rows)

    expected_schema = types.StructType(
        [
            types.StructField("field1", types.IntegerType(), False),
            types.StructField("field2", types.StringType(), False),
        ]
    )

    assertDataFrameEqual(
        df, spark.createDataFrame([(1, "a"), (2, "b")], expected_schema)
    )


def test_get_dataframe_with_nested_schemas(spark: SparkSession):
    @dataclass
    class NestedDataClass:
        nested_field1: int
        nested_field2: str

    @dataclass
    class MyDataClass:
        top_level: str
        nested: NestedDataClass

    rows = [
        MyDataClass("a", NestedDataClass(1, "x")),
        MyDataClass("b", NestedDataClass(2, "y")),
    ]
    df = get_dataframe(spark, MyDataClass, rows)

    expected_schema = types.StructType(
        [
            types.StructField("top_level", types.StringType(), False),
            types.StructField(
                "nested",
                types.StructType(
                    [
                        types.StructField("nested_field1", types.IntegerType(), False),
                        types.StructField("nested_field2", types.StringType(), False),
                    ]
                ),
                False,
            ),
        ]
    )

    assertDataFrameEqual(
        df, spark.createDataFrame([("a", (1, "x")), ("b", (2, "y"))], expected_schema)
    )


def test_get_dataframe_raises_value_error_for_rows_with_different_schemas(
    spark: SparkSession,
):

    @dataclass
    class MyDataClass:
        field1: int
        field2: str

    @dataclass
    class MyDataClass2:
        field1: int

    rows = [MyDataClass(1, "a"), MyDataClass2(2)]
    with pytest.raises(
        ValueError, match="Row 1 of type MyDataClass2. Expected type MyDataClass"
    ):
        get_dataframe(spark, MyDataClass, rows)


def test_get_dataframe_makes_spark_dataframe_from_pydantic_rows(spark: SparkSession):
    class Product(BaseModel):
        field1: int
        field2: str

    rows = [Product(field1=1, field2="a"), Product(field1=2, field2="b")]
    df = get_dataframe(spark, Product, rows)

    expected_schema = types.StructType(
        [
            types.StructField("field1", types.IntegerType(), True),
            types.StructField("field2", types.StringType(), True),
        ]
    )

    assertDataFrameEqual(
        df, spark.createDataFrame([(1, "a"), (2, "b")], expected_schema)
    )
