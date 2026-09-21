from dataclasses import dataclass
from typing import Annotated

from pyspark.sql import DataFrame, SparkSession
from spark_joinery import transform, Strict, Schema


@dataclass
class Customer:
    customer_id: str
    name: str
    is_active: bool


@dataclass
class CustomerCount:
    customer_count: int


@transform
def filter_active_customers(
    customers: Annotated[DataFrame, Strict(Customer)],
) -> Annotated[DataFrame, Strict(Customer)]:
    return customers.filter(customers.is_active)


@transform
def get_customer_count(
    customers: Annotated[DataFrame, Strict(Customer)],
) -> Annotated[DataFrame, Strict(CustomerCount)]:
    return customers.groupBy().count().withColumnRenamed("count", "customer_count")


spark = SparkSession.builder.appName("example").getOrCreate()

customer_schema = Schema(Customer)

customers = customer_schema.create_dataframe(
    spark,
    [
        Customer("1", "Alice", True),
        Customer("2", "Bob", False),
        Customer("3", "Charlie", True),
    ],
)

active_customer_count = customers.transform(filter_active_customers).transform(
    get_customer_count
)
active_customer_count.show()
