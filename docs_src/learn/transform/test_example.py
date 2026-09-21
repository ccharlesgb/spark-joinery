from dataclasses import dataclass
from typing import Annotated
from pyspark.testing import assertDataFrameEqual

from pyspark.sql import DataFrame, SparkSession

from spark_joinery import Schema, transform, Strict


@dataclass
class Customer:
    customer_id: str
    name: str
    is_active: bool


@transform
def filter_active_customers(
    customers: Annotated[DataFrame, Strict(Customer)],
) -> Annotated[DataFrame, Strict(Customer)]:
    return customers.filter(customers.is_active)


def test_filter_active_customers():
    spark = SparkSession.builder.appName("test").getOrCreate()
    customer_schema = Schema(Customer)

    customers = customer_schema.create_dataframe(
        spark,
        [
            Customer("1", "Alice", True),
            Customer("2", "Bob", False),
            Customer("3", "Charlie", True),
        ],
    )
    expected_active_customers = customer_schema.create_dataframe(
        spark,
        [
            Customer("1", "Alice", True),
            Customer("3", "Charlie", False),
        ],
    )

    active_customers = customers.transform(filter_active_customers)
    assertDataFrameEqual(active_customers, expected_active_customers)
