from typing import Annotated

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from .context import OrdersPath, CustomersPath, OutputPath, RunDate
from .schemas import Order, Customer, OrderWithCustomerDimension
from spark_joinery.collection import Collection
from spark_joinery.dependencies import Context
from dataclasses import fields


order_product = Collection()


@order_product.transform
def read_orders(
    spark: SparkSession, path: Annotated[OrdersPath, Context()]
) -> Annotated[DataFrame, Order]:
    return spark.read.parquet(path)


@order_product.transform
def filter_orders(
    orders: Annotated[DataFrame, Order], run_date: Annotated[RunDate, Context()]
) -> Annotated[DataFrame, Order]:
    return orders.filter(F.to_date(F.col("order_timestamp")) == F.lit(run_date))


@order_product.transform
def read_customers(
    spark: SparkSession, path: Annotated[CustomersPath, Context()]
) -> Annotated[DataFrame, Customer]:
    return spark.read.parquet(path)


@order_product.transform
def join_orders_with_customers(
    orders: Annotated[DataFrame, Order], customers: Annotated[DataFrame, Customer]
) -> Annotated[DataFrame, OrderWithCustomerDimension]:
    joined = orders.join(customers, on="customer_id", how="inner")
    output_columns = [field.name for field in fields(OrderWithCustomerDimension)]
    return joined.select(*output_columns)


@order_product.transform
def write_output(
    order_with_customer_dimension: Annotated[DataFrame, OrderWithCustomerDimension],
    path: Annotated[OutputPath, Context()],
) -> None:
    order_with_customer_dimension.write.mode("overwrite").parquet(path)
