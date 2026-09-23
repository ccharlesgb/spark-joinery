from dataclasses import dataclass
from datetime import date
from pathlib import Path
import traceback
from pyspark.sql import DataFrame, SparkSession
from typing import Annotated
from spark_joinery import Pipeline, Strict, transform, PipelineContext
from spark_joinery import Context


@dataclass
class A:
    snapshot_date: str


class RunDate(date):
    pass


@transform
def read_data(
    spark: SparkSession, run_date: Annotated[RunDate, Context()]
) -> Annotated[DataFrame, Strict(A)]:
    expected_path = Path(__file__).parent / f"example_{run_date.strftime('%Y%m%d')}.csv"
    return spark.read.csv(str(expected_path), header=True)


@transform
def write_data(b: Annotated[DataFrame, Strict(A)]) -> None:
    b.show()


order_metrics = Pipeline()
read_data_step = order_metrics.add_step(read_data)
write_data_step = order_metrics.add_step(write_data)

order_metrics.connect(read_data_step, write_data_step)

context = PipelineContext(values=[date(2026, 1, 1)])

try:
    order_metrics.run(spark=SparkSession.builder.getOrCreate(), context=context)
except Exception:
    print(traceback.format_exc(limit=1))
