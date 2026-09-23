import traceback

from spark_joinery import Pipeline
from dataclasses import dataclass
from pyspark.sql import DataFrame
from typing import Annotated
from spark_joinery import Strict, transform


@dataclass
class A: ...


@dataclass
class B: ...


@transform
def transform_data(
    _: Annotated[DataFrame, Strict(A)],
) -> Annotated[DataFrame, Strict(B)]: ...


@transform
def transform_data_back(
    _: Annotated[DataFrame, Strict(B)],
) -> Annotated[DataFrame, Strict(A)]: ...


order_metrics = Pipeline()
transform_data_step = order_metrics.add_step(transform_data)
transform_data_back_step = order_metrics.add_step(transform_data_back)

order_metrics.connect(transform_data_step, transform_data_back_step)
try:
    order_metrics.connect(transform_data_back_step, transform_data_step)
except Exception:
    print(traceback.format_exc(limit=1))
