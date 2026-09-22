from dataclasses import dataclass

from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType
from spark_joinery import Schema


@dataclass
class Customer:
    customer_id: str
    name: str


spark = SparkSession.builder.appName("OrderMetricsApp").getOrCreate()
df = spark.createDataFrame(
    [("1", "Alice", "England"), ("2", "Bob", "France")],
    schema=StructType(
        [
            StructField("customer_id", StringType(), nullable=True),
            StructField("name", StringType(), nullable=True),
            StructField("country", StringType(), nullable=True),
        ]
    ),
)

df = Schema(Customer).coerce_dataframe(df, mode="project_top_level")
df.show()
print(Schema(Customer).pretty_schema)
