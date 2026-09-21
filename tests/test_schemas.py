from dataclasses import dataclass
from typing import Annotated, Generator, Optional

import pytest
from pyspark.sql import SparkSession, types
from pyspark.testing import assertDataFrameEqual
from pydantic import BaseModel

from spark_joinery import schemas


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
    dataframe = schemas.get_dataframe(spark, MyDataClass, rows)

    expected_schema = types.StructType(
        [
            types.StructField("field1", types.LongType(), False),
            types.StructField("field2", types.StringType(), False),
        ]
    )

    assertDataFrameEqual(
        dataframe, spark.createDataFrame([(1, "a"), (2, "b")], expected_schema)
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
    dataframe = schemas.get_dataframe(spark, MyDataClass, rows)

    expected_schema = types.StructType(
        [
            types.StructField("top_level", types.StringType(), False),
            types.StructField(
                "nested",
                types.StructType(
                    [
                        types.StructField("nested_field1", types.LongType(), False),
                        types.StructField("nested_field2", types.StringType(), False),
                    ]
                ),
                False,
            ),
        ]
    )

    assertDataFrameEqual(
        dataframe,
        spark.createDataFrame([("a", (1, "x")), ("b", (2, "y"))], expected_schema),
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
        schemas.get_dataframe(spark, MyDataClass, rows)


def test_get_dataframe_makes_spark_dataframe_from_pydantic_rows(spark: SparkSession):
    class Product(BaseModel):
        field1: int
        field2: str

    rows = [Product(field1=1, field2="a"), Product(field1=2, field2="b")]
    dataframe = schemas.get_dataframe(spark, Product, rows)

    expected_schema = types.StructType(
        [
            types.StructField("field1", types.LongType(), True),
            types.StructField("field2", types.StringType(), True),
        ]
    )

    assertDataFrameEqual(
        dataframe, spark.createDataFrame([(1, "a"), (2, "b")], expected_schema)
    )


def test_get_spark_schema_from_model_raises_value_error_for_non_schema_model():
    class NotADataclass:
        pass

    with pytest.raises(
        ValueError, match="NotADataclass is neither a dataclass nor a pydantic model"
    ):
        schemas.get_spark_schema_from_model(NotADataclass)


def test_get_spark_schema_from_model_returns_struct_type_for_dataclass():
    @dataclass
    class MyDataClass:
        field1: int
        field2: str

    actual_schema = schemas.get_spark_schema_from_model(MyDataClass)
    expected_schema = types.StructType(
        [
            types.StructField("field1", types.LongType(), True),
            types.StructField("field2", types.StringType(), True),
        ]
    )
    assert actual_schema == expected_schema


def test_get_spark_schema_from_model_honors_annotated_spark_type():
    @dataclass
    class MyDataClass:
        field1: Annotated[float, types.DoubleType()]

    actual_schema = schemas.get_spark_schema_from_model(MyDataClass)

    assert actual_schema == types.StructType(
        [types.StructField("field1", types.DoubleType(), True)]
    )


def test_get_spark_schema_from_model_honors_annotated_array_element_type():
    @dataclass
    class MyDataClass:
        field1: list[Annotated[float, types.DoubleType()]]

    actual_schema = schemas.get_spark_schema_from_model(MyDataClass)

    assert actual_schema == types.StructType(
        [types.StructField("field1", types.ArrayType(types.DoubleType(), True), True)]
    )


def test_get_spark_schema_from_model_returns_struct_type_for_dataclass_with_optional_fields():
    @dataclass
    class MyDataClass:
        field1: Optional[int]
        field2: str | None

    actual_schema = schemas.get_spark_schema_from_model(MyDataClass)
    expected_schema = types.StructType(
        [
            types.StructField("field1", types.LongType(), True),
            types.StructField("field2", types.StringType(), True),
        ]
    )
    assert actual_schema == expected_schema


def test_get_spark_schema_from_model_returns_nested_schema():
    @dataclass
    class NestedDataClass:
        nested_field1: int
        nested_field2: str

    @dataclass
    class MyDataClass:
        field1: NestedDataClass
        field2: str

    actual_schema = schemas.get_spark_schema_from_model(MyDataClass)
    expected_schema = types.StructType(
        [
            types.StructField(
                "field1",
                types.StructType(
                    [
                        types.StructField("nested_field1", types.LongType(), True),
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

    actual_schema = schemas.get_spark_schema_from_model(MyDataClass)
    expected_schema = types.StructType(
        [
            types.StructField(
                "array_field", types.ArrayType(types.StringType(), True), True
            ),
            types.StructField(
                "optional_array_field", types.ArrayType(types.LongType(), True), True
            ),
            types.StructField(
                "array_field_with_optional_elements",
                types.ArrayType(types.DoubleType(), True),
                True,
            ),
            types.StructField(
                "array_field_of_datclasses",
                types.ArrayType(
                    types.StructType(
                        [types.StructField("nested_field", types.LongType(), True)]
                    ),
                    True,
                ),
                True,
            ),
            types.StructField(
                "array_field_of_optional_datclasses",
                types.ArrayType(
                    types.StructType(
                        [types.StructField("nested_field", types.LongType(), True)]
                    ),
                    True,
                ),
                True,
            ),
            types.StructField(
                "optional_array_field_of_optional_datclasses",
                types.ArrayType(
                    types.StructType(
                        [types.StructField("nested_field", types.LongType(), True)]
                    ),
                    True,
                ),
                True,
            ),
        ]
    )
    assert actual_schema == expected_schema


def test_get_spark_schema_from_model_returns_struct_type_for_pydantic_model():
    class Product(BaseModel):
        product_id: int
        product_name: str

    actual_schema = schemas.get_spark_schema_from_model(Product)
    expected_schema = types.StructType(
        [
            types.StructField("product_id", types.LongType(), True),
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
            types.StructField("order_id", types.LongType(), True),
            types.StructField(
                "products",
                types.ArrayType(
                    types.StructType(
                        [types.StructField("product_id", types.LongType(), True)]
                    ),
                    True,
                ),
                True,
            ),
        ]
    )
    assert actual_schema == expected_schema


def test_schema_diff_reports_all_nested_differences():
    given = types.StructType(
        [
            types.StructField("extra", types.StringType(), True),
            types.StructField(
                "customer",
                types.StructType(
                    [
                        types.StructField("name", types.IntegerType(), False),
                        types.StructField("legacy", types.StringType(), True),
                    ]
                ),
                False,
            ),
            types.StructField(
                "items", types.ArrayType(types.IntegerType(), containsNull=False), True
            ),
        ]
    )
    expected = types.StructType(
        [
            types.StructField(
                "customer",
                types.StructType(
                    [
                        types.StructField("name", types.StringType(), True),
                        types.StructField("postcode", types.StringType(), True),
                    ]
                ),
                True,
            ),
            types.StructField(
                "items", types.ArrayType(types.StringType(), containsNull=True), True
            ),
        ]
    )

    diff = schemas._schema_diff(given, expected)

    assert [difference.path for difference in diff.additional] == [
        "extra",
        "customer.legacy",
    ]
    assert [difference.path for difference in diff.missing] == ["customer.postcode"]
    assert [difference.path for difference in diff.type_mismatches] == [
        "customer.name",
        "items[]",
    ]
    assert [difference.path for difference in diff.nullable_mismatches] == [
        "customer",
        "customer.name",
        "items[]",
    ]


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

_TEST_CAST_MATRIX = {
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


@pytest.mark.parametrize(
    ("source", "target", "allowed"),
    [
        (source, target, allowed)
        for (source, target), allowed in _TEST_CAST_MATRIX.items()
    ],
)
def test_is_cast_compatible_matches_ansi_matrix(source, target, allowed):
    assert schemas._is_cast_compatible(source, target) is allowed


def test_schema_coercion_error_exposes_all_violations():
    violations = (
        schemas.SchemaDifference(
            "missing",
            "customer.postcode",
            None,
            types.StringType(),
            None,
            True,
        ),
        schemas.SchemaDifference(
            "type_mismatch",
            "active",
            types.BinaryType(),
            types.BooleanType(),
            True,
            True,
        ),
    )

    error = schemas.SchemaCoercionError("project_all_cast", violations)

    assert error.mode == "project_all_cast"
    assert error.violations == violations
    assert str(error) == (
        "Cannot coerce dataframe using mode 'project_all_cast':\n"
        "  Missing fields:\n"
        "    - customer.postcode (expected StringType())\n"
        "  Unsupported type casts:\n"
        "    - active: BinaryType() -> BooleanType()"
    )


# --- coerce_dataframe: strict ---


def test_coerce_dataframe_strict_passes_when_schemas_match_apart_from_nullable(
    spark: SparkSession,
):
    dataframe = spark.createDataFrame(
        [(1, "a")],
        types.StructType(
            [
                types.StructField("a", types.IntegerType(), False),
                types.StructField("b", types.StringType(), True),
            ]
        ),
    )
    schema = types.StructType(
        [
            types.StructField("a", types.IntegerType(), True),
            types.StructField("b", types.StringType(), True),
        ]
    )

    result = schemas.coerce_dataframe(dataframe, schema, "strict")

    assert result.schema == dataframe.schema


def test_coerce_dataframe_strict_null_raises_for_nullable_mismatch(
    spark: SparkSession,
):
    dataframe = spark.createDataFrame(
        [(1,)],
        types.StructType([types.StructField("value", types.IntegerType(), False)]),
    )
    schema = types.StructType([types.StructField("value", types.IntegerType(), True)])

    with pytest.raises(schemas.SchemaCoercionError) as error:
        schemas.coerce_dataframe(dataframe, schema, "strict_null")

    assert [
        (violation.kind, violation.path) for violation in error.value.violations
    ] == [("nullable_mismatch", "value")]


def test_coerce_dataframe_strict_raises_for_extra_column(spark: SparkSession):
    dataframe = spark.createDataFrame(
        [(1, "a")],
        types.StructType(
            [
                types.StructField("a", types.IntegerType(), True),
                types.StructField("b", types.StringType(), True),
            ]
        ),
    )
    schema = types.StructType([types.StructField("a", types.IntegerType(), True)])

    with pytest.raises(schemas.SchemaCoercionError):
        schemas.coerce_dataframe(dataframe, schema, "strict")


def test_coerce_dataframe_strict_raises_for_type_mismatch(spark: SparkSession):
    dataframe = spark.createDataFrame(
        [(1,)], types.StructType([types.StructField("a", types.IntegerType(), True)])
    )
    schema = types.StructType([types.StructField("a", types.StringType(), True)])

    with pytest.raises(schemas.SchemaCoercionError):
        schemas.coerce_dataframe(dataframe, schema, "strict")


# --- coerce_dataframe: project ---


def test_coerce_dataframe_project_selects_subset_of_columns(spark: SparkSession):
    dataframe = spark.createDataFrame(
        [(1, "a", 1.0)],
        types.StructType(
            [
                types.StructField("a", types.IntegerType(), True),
                types.StructField("b", types.StringType(), True),
                types.StructField("c", types.DoubleType(), True),
            ]
        ),
    )
    schema = types.StructType(
        [
            types.StructField("a", types.IntegerType(), True),
            types.StructField("b", types.StringType(), True),
        ]
    )

    result = schemas.coerce_dataframe(dataframe, schema, "project")

    assert result.columns == ["a", "b"]
    assert result.collect() == [types.Row(a=1, b="a")]


def test_coerce_dataframe_project_raises_for_missing_column(spark: SparkSession):
    dataframe = spark.createDataFrame(
        [(1,)], types.StructType([types.StructField("a", types.IntegerType(), True)])
    )
    schema = types.StructType(
        [
            types.StructField("a", types.IntegerType(), True),
            types.StructField("b", types.StringType(), True),
        ]
    )

    with pytest.raises(schemas.SchemaCoercionError):
        schemas.coerce_dataframe(dataframe, schema, "project")


def test_coerce_dataframe_project_raises_for_type_mismatch(spark: SparkSession):
    dataframe = spark.createDataFrame(
        [(1,)], types.StructType([types.StructField("a", types.IntegerType(), True)])
    )
    schema = types.StructType([types.StructField("a", types.StringType(), True)])

    with pytest.raises(schemas.SchemaCoercionError):
        schemas.coerce_dataframe(dataframe, schema, "project")


def test_coerce_dataframe_project_reports_every_missing_and_type_mismatch(
    spark: SparkSession,
):
    dataframe = spark.createDataFrame(
        [(1,)],
        types.StructType([types.StructField("actual", types.IntegerType(), True)]),
    )
    schema = types.StructType(
        [
            types.StructField("actual", types.StringType(), True),
            types.StructField("required", types.StringType(), True),
        ]
    )

    with pytest.raises(schemas.SchemaCoercionError) as error:
        schemas.coerce_dataframe(dataframe, schema, "project")

    assert [
        (violation.kind, violation.path) for violation in error.value.violations
    ] == [
        ("missing", "required"),
        ("type_mismatch", "actual"),
    ]


# --- coerce_dataframe: project_all ---


def test_coerce_dataframe_project_all_prunes_nested_struct_fields(spark: SparkSession):
    dataframe = spark.createDataFrame(
        [((1, 2),)],
        types.StructType(
            [
                types.StructField(
                    "a",
                    types.StructType(
                        [
                            types.StructField("a1", types.IntegerType(), True),
                            types.StructField("a2", types.IntegerType(), True),
                        ]
                    ),
                    True,
                )
            ]
        ),
    )
    schema = types.StructType(
        [
            types.StructField(
                "a",
                types.StructType([types.StructField("a1", types.IntegerType(), True)]),
                True,
            )
        ]
    )

    result = schemas.coerce_dataframe(dataframe, schema, "project_all")

    assert result.collect() == [types.Row(a=types.Row(a1=1))]


def test_coerce_dataframe_project_all_prunes_array_of_struct_fields(
    spark: SparkSession,
):
    dataframe = spark.createDataFrame(
        [([(1, 2), (3, 4)],)],
        types.StructType(
            [
                types.StructField(
                    "items",
                    types.ArrayType(
                        types.StructType(
                            [
                                types.StructField("a1", types.IntegerType(), True),
                                types.StructField("a2", types.IntegerType(), True),
                            ]
                        )
                    ),
                    True,
                )
            ]
        ),
    )
    schema = types.StructType(
        [
            types.StructField(
                "items",
                types.ArrayType(
                    types.StructType(
                        [types.StructField("a1", types.IntegerType(), True)]
                    )
                ),
                True,
            )
        ]
    )

    result = schemas.coerce_dataframe(dataframe, schema, "project_all")

    assert result.collect() == [types.Row(items=[types.Row(a1=1), types.Row(a1=3)])]


def test_coerce_dataframe_project_all_raises_for_missing_nested_field(
    spark: SparkSession,
):
    dataframe = spark.createDataFrame(
        [((1,),)],
        types.StructType(
            [
                types.StructField(
                    "a",
                    types.StructType(
                        [types.StructField("a1", types.IntegerType(), True)]
                    ),
                    True,
                )
            ]
        ),
    )
    schema = types.StructType(
        [
            types.StructField(
                "a",
                types.StructType(
                    [
                        types.StructField("a1", types.IntegerType(), True),
                        types.StructField("a2", types.IntegerType(), True),
                    ]
                ),
                True,
            )
        ]
    )

    with pytest.raises(schemas.SchemaCoercionError):
        schemas.coerce_dataframe(dataframe, schema, "project_all")


def test_coerce_dataframe_project_all_raises_for_nested_type_mismatch(
    spark: SparkSession,
):
    dataframe = spark.createDataFrame(
        [((1,),)],
        types.StructType(
            [
                types.StructField(
                    "a",
                    types.StructType(
                        [types.StructField("a1", types.IntegerType(), True)]
                    ),
                    True,
                )
            ]
        ),
    )
    schema = types.StructType(
        [
            types.StructField(
                "a",
                types.StructType([types.StructField("a1", types.StringType(), True)]),
                True,
            )
        ]
    )

    with pytest.raises(schemas.SchemaCoercionError):
        schemas.coerce_dataframe(dataframe, schema, "project_all")


# --- coerce_dataframe: project_all_cast ---


def test_coerce_dataframe_project_all_cast_casts_mismatched_leaf_types(
    spark: SparkSession,
):
    dataframe = spark.createDataFrame(
        [(1.9,)], types.StructType([types.StructField("a", types.DoubleType(), True)])
    )
    schema = types.StructType([types.StructField("a", types.IntegerType(), True)])

    result = schemas.coerce_dataframe(dataframe, schema, "project_all_cast")

    assert result.collect() == [types.Row(a=1)]


def test_coerce_dataframe_project_all_cast_casts_nested_struct_fields(
    spark: SparkSession,
):
    dataframe = spark.createDataFrame(
        [((1.9,),)],
        types.StructType(
            [
                types.StructField(
                    "a",
                    types.StructType(
                        [types.StructField("a1", types.DoubleType(), True)]
                    ),
                    True,
                )
            ]
        ),
    )
    schema = types.StructType(
        [
            types.StructField(
                "a",
                types.StructType([types.StructField("a1", types.IntegerType(), True)]),
                True,
            )
        ]
    )

    result = schemas.coerce_dataframe(dataframe, schema, "project_all_cast")

    assert result.collect() == [types.Row(a=types.Row(a1=1))]


def test_coerce_dataframe_project_all_cast_casts_array_of_struct_fields(
    spark: SparkSession,
):
    dataframe = spark.createDataFrame(
        [([(1.9,), (2.1,)],)],
        types.StructType(
            [
                types.StructField(
                    "items",
                    types.ArrayType(
                        types.StructType(
                            [types.StructField("a1", types.DoubleType(), True)]
                        )
                    ),
                    True,
                )
            ]
        ),
    )
    schema = types.StructType(
        [
            types.StructField(
                "items",
                types.ArrayType(
                    types.StructType(
                        [types.StructField("a1", types.IntegerType(), True)]
                    )
                ),
                True,
            )
        ]
    )

    result = schemas.coerce_dataframe(dataframe, schema, "project_all_cast")

    assert result.collect() == [types.Row(items=[types.Row(a1=1), types.Row(a1=2)])]


def test_coerce_dataframe_project_all_cast_raises_for_missing_field(
    spark: SparkSession,
):
    dataframe = spark.createDataFrame(
        [(1,)], types.StructType([types.StructField("a", types.IntegerType(), True)])
    )
    schema = types.StructType(
        [
            types.StructField("a", types.IntegerType(), True),
            types.StructField("b", types.StringType(), True),
        ]
    )

    with pytest.raises(schemas.SchemaCoercionError):
        schemas.coerce_dataframe(dataframe, schema, "project_all_cast")


def test_coerce_dataframe_project_all_cast_reports_every_unsupported_cast(
    spark: SparkSession,
):
    dataframe = spark.createDataFrame(
        [(None, None)],
        types.StructType(
            [
                types.StructField("binary_value", types.BinaryType(), True),
                types.StructField("date_value", types.DateType(), True),
            ]
        ),
    )
    schema = types.StructType(
        [
            types.StructField("binary_value", types.BooleanType(), True),
            types.StructField("date_value", types.IntegerType(), True),
            types.StructField("required", types.StringType(), True),
        ]
    )

    with pytest.raises(schemas.SchemaCoercionError) as error:
        schemas.coerce_dataframe(dataframe, schema, "project_all_cast")

    assert [
        (violation.kind, violation.path) for violation in error.value.violations
    ] == [
        ("missing", "required"),
        ("type_mismatch", "binary_value"),
        ("type_mismatch", "date_value"),
    ]
