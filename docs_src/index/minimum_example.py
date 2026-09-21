from dataclasses import dataclass
from datetime import date, datetime
from pyspark.sql import DataFrame, SparkSession
from typing import Annotated
from pyspark.sql.types import DoubleType
from spark_joinery import Pipeline, Schema


@dataclass
class Orders:
    order_timestamp: datetime
    customer_id: str
    order_value: float


@dataclass
class OrderMetrics:
    order_date: date
    customer_id: str
    total_order_value: Annotated[float, DoubleType()]


order_metrics = Pipeline()


@order_metrics.transform
def read_orders(spark: SparkSession) -> Annotated[DataFrame, Orders]:
    input_schema = Schema(Orders).spark_schema
    return spark.createDataFrame(
        [
            (datetime(2026, 1, 1, 0, 0, 0), "customer_1", 10.0),
            (datetime(2026, 1, 2, 0, 0, 0), "customer_2", 20.0),
            (datetime(2026, 1, 2, 0, 0, 0), "customer_2", 30.0),
        ],
        schema=input_schema,
    )


@order_metrics.transform
def get_metrics(
    order_table: Annotated[DataFrame, Orders],
) -> Annotated[DataFrame, OrderMetrics]:
    return (
        order_table.groupBy(
            order_table["order_timestamp"].cast("date").alias("order_date"),
            order_table["customer_id"],
        )
        .sum("order_value")
        .withColumnRenamed("sum(order_value)", "total_order_value")
    )


@order_metrics.transform
def print_metrics(metrics_table: Annotated[DataFrame, OrderMetrics]) -> None:
    metrics_table.show()


read_orders_step = order_metrics.add_step(read_orders)
get_metrics_step = order_metrics.add_step(get_metrics)
print_metrics_step = order_metrics.add_step(print_metrics)

read_orders_step >> get_metrics_step >> print_metrics_step

spark = SparkSession.builder.appName("OrderMetricsApp").getOrCreate()

validated = order_metrics.validate()
validated.run(spark)
