from dataclasses import dataclass

from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType
from spark_joinery import coerce_dataframe_to_model, pretty_print_struct_type


@dataclass
class Customer:
    customer_id: int
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

df = coerce_dataframe_to_model(df, Customer, mode="coerce")
df.show()
pretty_print_struct_type(df.schema)
