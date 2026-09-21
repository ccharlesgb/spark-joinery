from typing import Annotated

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from .context import CustomersPath, OutputPath
from .schemas import Customer, CustomerGroup
from spark_joinery.collection import Collection
from spark_joinery.dependencies import Context
from spark_joinery import Strict


customer_group = Collection()


@customer_group.transform
def read_customers(
    spark: SparkSession, path: Annotated[CustomersPath, Context()]
) -> Annotated[DataFrame, Strict(Customer)]:
    return spark.read.parquet(path)


@customer_group.transform
def denormalise_group_id(
    customers: Annotated[DataFrame, Strict(Customer)],
) -> Annotated[DataFrame, Strict(CustomerGroup)]:
    customer = customers.alias("customer")
    group = customers.alias("group")

    return customer.join(
        group,
        F.col("customer.parent_customer_id") == F.col("group.customer_id"),
        "left",
    ).select(
        F.col("customer.customer_id"),
        F.col("customer.name"),
        F.col("customer.parent_customer_id"),
        F.col("group.name").alias("parent_name"),
    )


@customer_group.transform
def write_output(
    order_with_customer_dimension: Annotated[DataFrame, Strict(CustomerGroup)],
    path: Annotated[OutputPath, Context()],
) -> None:
    order_with_customer_dimension.write.mode("overwrite").parquet(path)
