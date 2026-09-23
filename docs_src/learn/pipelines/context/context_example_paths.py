from dataclasses import dataclass
from pathlib import Path
from pyspark.sql import DataFrame, SparkSession
from typing import Annotated
from spark_joinery import Pipeline, Strict, transform, PipelineContext
from spark_joinery import Context


@dataclass
class A:
    a: str


@dataclass
class B:
    b: str


@dataclass
class PathConfig:
    input_path: Path
    output_path: Path


@transform
def read_data(
    spark: SparkSession, path_config: Annotated[PathConfig, Context()]
) -> Annotated[DataFrame, Strict(A)]:
    return spark.read.csv(str(path_config.input_path), header=True)


@transform
def transform_data(
    a: Annotated[DataFrame, Strict(A)],
) -> Annotated[DataFrame, Strict(B)]:
    return a.withColumnRenamed("a", "b")


@transform
def write_data(
    b: Annotated[DataFrame, Strict(B)],
    path_config: Annotated[PathConfig, Context()],
) -> None:
    print("Write to ", path_config.output_path.name)
    b.show()


order_metrics = Pipeline()
read_data_step = order_metrics.add_step(read_data)
transform_data_step = order_metrics.add_step(transform_data)
write_data_step = order_metrics.add_step(write_data)

order_metrics.connect(read_data_step, transform_data_step)
order_metrics.connect(transform_data_step, write_data_step)

input_path = Path(__file__).parent / "example.csv"
output_path = Path(__file__).parent / "output.parquet"

path_config = PathConfig(input_path=input_path, output_path=output_path)
context = PipelineContext(values=[path_config])

order_metrics.run(spark=SparkSession.builder.getOrCreate(), context=context)
