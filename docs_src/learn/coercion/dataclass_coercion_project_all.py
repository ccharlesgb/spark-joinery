from dataclasses import dataclass

from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType
from spark_joinery import coerce_dataframe_to_model, pretty_print_struct_type


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
            StructField("customer_id", IntegerType(), nullable=True),
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

df = coerce_dataframe_to_model(df, Customer, mode="project_all")
df.show()
pretty_print_struct_type(df.schema)
