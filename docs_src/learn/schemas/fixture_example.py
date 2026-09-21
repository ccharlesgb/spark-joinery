from dataclasses import dataclass
from pyspark.sql import SparkSession
from spark_joinery import Schema


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

schema = Schema(Customer)
df = schema.create_dataframe(spark, rows)
df.show()
