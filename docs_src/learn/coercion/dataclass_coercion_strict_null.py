from dataclasses import dataclass
import traceback

from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType
from spark_joinery import Schema


@dataclass
class Customer:
    customer_id: str
    name: str


spark = SparkSession.builder.appName("OrderMetricsApp").getOrCreate()
df = spark.createDataFrame(
    [("1", "Alice")],
    schema=StructType(
        [
            StructField("customer_id", StringType(), nullable=False),
            StructField("name", StringType(), nullable=False),
        ]
    ),
)

try:
    Schema(Customer).coerce_dataframe(df, mode="strict_null")
except Exception:
    print(traceback.format_exc(limit=1))
