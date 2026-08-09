from dataclasses import dataclass
from typing import Annotated

from pyspark.sql import DataFrame, SparkSession

from pyspark_schemas.dependencies import Context
from pyspark_schemas.transform import _inspect_transform


@dataclass
class Order:
    order_id: int


@dataclass(frozen=True)
class PathConfig:
    value: str


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
        orders: Annotated[DataFrame, Order],
        path: Annotated[PathConfig, Context()],
    ) -> Annotated[DataFrame, Order]:
        raise AssertionError

    spec = _inspect_transform(filter_orders)

    assert set(spec.input_schemas) == {"orders"}
    assert set(spec.context_parameters) == {"path"}


def test_inspect_transform_with_no_context_parameters_has_empty_dict():
    def read_orders(spark: SparkSession) -> Annotated[DataFrame, Order]:
        raise AssertionError

    spec = _inspect_transform(read_orders)

    assert spec.context_parameters == {}
