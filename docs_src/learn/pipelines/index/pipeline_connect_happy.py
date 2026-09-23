from dataclasses import dataclass
from pyspark.sql import DataFrame, SparkSession
from typing import Annotated
from spark_joinery import Pipeline, Strict, transform


@dataclass
class A:
    a: str


@dataclass
class B:
    b: str


@transform
def read_data(spark: SparkSession) -> Annotated[DataFrame, Strict(A)]:
    return spark.createDataFrame([{"a": "value"}])


@transform
def transform_data(
    a: Annotated[DataFrame, Strict(A)],
) -> Annotated[DataFrame, Strict(B)]:
    return a.withColumnRenamed("a", "b")


@transform
def write_data(
    b: Annotated[DataFrame, Strict(B)],
) -> None:
    b.show()


order_metrics = Pipeline()
read_data_step = order_metrics.add_step(read_data)
transform_data_step = order_metrics.add_step(transform_data)
write_data_step = order_metrics.add_step(write_data)

order_metrics.connect(read_data_step, transform_data_step)
order_metrics.connect(transform_data_step, write_data_step)

order_metrics.run(spark=SparkSession.builder.getOrCreate())
