from dataclasses import dataclass

from pyspark.sql import SparkSession
from pyspark.sql.types import LongType, StringType, StructField, StructType
from spark_joinery import Schema


@dataclass
class Address:
    city: str


@dataclass
class Customer:
    customer_id: int
    address: Address


spark = SparkSession.builder.appName("OrderMetricsApp").getOrCreate()
df = spark.createDataFrame(
    [(1, ("London", "England")), (2, ("Paris", "France"))],
    schema=StructType(
        [
            StructField("customer_id", LongType(), nullable=True),
            StructField(
                "address",
                StructType(
                    [
                        StructField("city", StringType(), nullable=True),
                        StructField("country", StringType(), nullable=True),
                    ]
                ),
                nullable=True,
            ),
        ]
    ),
)

df = Schema(Customer).coerce_dataframe(df, mode="project_all")
df.show()
print(Schema(Customer).pretty_schema)
