from dataclasses import dataclass
from pyspark.sql import SparkSession
from spark_joinery import get_dataframe


@dataclass
class Customer:
    customer_id: str
    name: str
    is_active: bool


spark = SparkSession.builder.appName("example").getOrCreate()

rows = [
    Customer("1", "Alice", True),
    Customer("2", "Bob", False),
    Customer("3", "Charlie", True),
]
df = get_dataframe(spark, Customer, rows)
df.show()
