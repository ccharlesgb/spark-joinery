from pyspark.sql import SparkSession

from dataclasses import dataclass
import traceback
from pyspark.sql.types import StringType, StructField, StructType
from spark_joinery import coerce_dataframe_to_model


@dataclass
class Customer:
    customer_id: str
    name: str


spark = SparkSession.builder.appName("OrderMetricsApp").getOrCreate()
df = spark.createDataFrame(
    [
        ("1", "Alice", "England"),
        ("2", "Bob", "France"),
    ],
    schema=StructType(
        [
            StructField("customer_id", StringType(), nullable=True),
            StructField("name", StringType(), nullable=True),
            StructField("country", StringType(), nullable=True),
        ]
    ),
)

try:
    coerce_dataframe_to_model(df, Customer, mode="strict")
except Exception:
    print(traceback.format_exc(limit=1))
