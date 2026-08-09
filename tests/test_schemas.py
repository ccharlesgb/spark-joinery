from dataclasses import dataclass
from typing import Generator, Optional

import pytest
from pyspark.sql import SparkSession, types
from pydantic import BaseModel

from pyspark_schemas import schemas


@pytest.fixture(scope="session")
def spark() -> Generator[SparkSession, None, None]:
    spark = (
        SparkSession.builder.appName("pyspark-schemas-tests")
        .master("local[*]")
        .getOrCreate()
    )
    yield spark
    spark.stop()


def test_get_spark_schema_from_dataclass_raises_value_error_for_non_dataclass():
    class NotADataclass:
        pass

    with pytest.raises(ValueError, match="NotADataclass is not a dataclass"):
        schemas.get_spark_schema_from_dataclass(NotADataclass)


def test_get_spark_schema_from_dataclass_returns_struct_type_for_dataclass():
    @dataclass
    class MyDataClass:
        field1: int
        field2: str

    actual_schema = schemas.get_spark_schema_from_dataclass(MyDataClass)
    expected_schema = types.StructType(
        [
            types.StructField("field1", types.IntegerType(), True),
            types.StructField("field2", types.StringType(), True),
        ]
    )
    assert actual_schema == expected_schema


def test_get_spark_schema_from_dataclass_returns_struct_type_for_dataclass_with_optional_fields():
    @dataclass
    class MyDataClass:
        field1: Optional[int]
        field2: str | None

    actual_schema = schemas.get_spark_schema_from_dataclass(MyDataClass)
    expected_schema = types.StructType(
        [
            types.StructField("field1", types.IntegerType(), True),
            types.StructField("field2", types.StringType(), True),
        ]
    )
    assert actual_schema == expected_schema


def test_get_spark_schema_from_dataclass_returns_nested_schema():
    @dataclass
    class NestedDataClass:
        nested_field1: int
        nested_field2: str

    @dataclass
    class MyDataClass:
        field1: NestedDataClass
        field2: str

    actual_schema = schemas.get_spark_schema_from_dataclass(MyDataClass)
    expected_schema = types.StructType(
        [
            types.StructField(
                "field1",
                types.StructType(
                    [
                        types.StructField("nested_field1", types.IntegerType(), True),
                        types.StructField("nested_field2", types.StringType(), True),
                    ]
                ),
                True,
            ),
            types.StructField("field2", types.StringType(), True),
        ]
    )
    assert actual_schema == expected_schema


def test_get_spark_schema_with_array_fields():
    @dataclass
    class NestedDataClass:
        nested_field: int

    @dataclass
    class MyDataClass:
        array_field: list[str]
        optional_array_field: Optional[list[int]]
        array_field_with_optional_elements: list[Optional[float]]
        array_field_of_datclasses: list[NestedDataClass]
        array_field_of_optional_datclasses: list[Optional[NestedDataClass]]
        optional_array_field_of_optional_datclasses: Optional[
            list[Optional[NestedDataClass]]
        ]

    actual_schema = schemas.get_spark_schema_from_dataclass(MyDataClass)
    expected_schema = types.StructType(
        [
            types.StructField(
                "array_field", types.ArrayType(types.StringType(), True), True
            ),
            types.StructField(
                "optional_array_field", types.ArrayType(types.IntegerType(), True), True
            ),
            types.StructField(
                "array_field_with_optional_elements",
                types.ArrayType(types.FloatType(), True),
                True,
            ),
            types.StructField(
                "array_field_of_datclasses",
                types.ArrayType(
                    types.StructType(
                        [types.StructField("nested_field", types.IntegerType(), True)]
                    ),
                    True,
                ),
                True,
            ),
            types.StructField(
                "array_field_of_optional_datclasses",
                types.ArrayType(
                    types.StructType(
                        [types.StructField("nested_field", types.IntegerType(), True)]
                    ),
                    True,
                ),
                True,
            ),
            types.StructField(
                "optional_array_field_of_optional_datclasses",
                types.ArrayType(
                    types.StructType(
                        [types.StructField("nested_field", types.IntegerType(), True)]
                    ),
                    True,
                ),
                True,
            ),
        ]
    )
    assert actual_schema == expected_schema


def test_dataframe_is_schema_returns_true_for_matching_schema(spark: SparkSession):
    @dataclass
    class MyDataClass:
        field1: int
        field2: str

    schema = schemas.get_spark_schema_from_dataclass(MyDataClass)
    dataframe = spark.createDataFrame([(1, "a")], schema)

    assert schemas.dataframe_is_schema(dataframe, MyDataClass)


def test_dataframe_is_schema_returns_false_for_non_matching_schema(spark: SparkSession):
    @dataclass
    class MyDataClass:
        field1: int
        field2: str

    dataframe = spark.createDataFrame(
        [(1,)],
        types.StructType([types.StructField("field1", types.IntegerType(), False)]),
    )

    assert not schemas.dataframe_is_schema(dataframe, MyDataClass)


def test_dataframe_is_schema_raises_for_non_dataclass(spark: SparkSession):
    class NotADataclass:
        pass

    dataframe = spark.createDataFrame(
        [(1,)],
        types.StructType([types.StructField("field1", types.IntegerType(), False)]),
    )

    with pytest.raises(ValueError, match="NotADataclass is not a dataclass"):
        schemas.dataframe_is_schema(dataframe, NotADataclass)


def test_get_spark_schema_from_model_returns_struct_type_for_pydantic_model():
    class Product(BaseModel):
        product_id: int
        product_name: str

    actual_schema = schemas.get_spark_schema_from_model(Product)
    expected_schema = types.StructType(
        [
            types.StructField("product_id", types.IntegerType(), True),
            types.StructField("product_name", types.StringType(), True),
        ]
    )
    assert actual_schema == expected_schema


def test_get_spark_schema_from_model_returns_nested_schema_for_pydantic_models():
    class Product(BaseModel):
        product_id: int

    class Order(BaseModel):
        order_id: int
        products: list[Product]

    actual_schema = schemas.get_spark_schema_from_model(Order)
    expected_schema = types.StructType(
        [
            types.StructField("order_id", types.IntegerType(), True),
            types.StructField(
                "products",
                types.ArrayType(
                    types.StructType(
                        [types.StructField("product_id", types.IntegerType(), True)]
                    ),
                    True,
                ),
                True,
            ),
        ]
    )
    assert actual_schema == expected_schema


def test_dataframe_is_model_schema_returns_true_for_pydantic_model(
    spark: SparkSession,
):
    class Product(BaseModel):
        product_id: int
        product_name: str

    schema = schemas.get_spark_schema_from_model(Product)
    dataframe = spark.createDataFrame([(1, "a")], schema)

    assert schemas.dataframe_is_model_schema(dataframe, Product)
