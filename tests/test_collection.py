from dataclasses import dataclass
from typing import Annotated

from pyspark.sql import DataFrame

from spark_joinery import schemas
from spark_joinery.collection import Collection


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
