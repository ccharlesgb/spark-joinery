from dataclasses import dataclass
from typing import Annotated

from pyspark.sql import DataFrame, SparkSession
from spark_joinery import transform


@dataclass
class Customer:
    customer_id: str
    name: str
    is_active: bool


@transform
def filter_active_customers(
    customers: Annotated[DataFrame, Customer],
) -> Annotated[DataFrame, Customer]:
    return customers.filter(customers.is_active)


spark = SparkSession.builder.appName("example").getOrCreate()

customers = spark.createDataFrame(
    [
        ("1", "Alice", True),
        ("2", "Bob", False),
        ("3", "Charlie", True),
    ],
    ["customer_id", "name", "is_active"],
)

active_customers = filter_active_customers(customers)
active_customers.show()
