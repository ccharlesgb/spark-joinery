from typing import Annotated

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import timedelta

from .context import SnapshottedDimensionPath, OutputPath, RunDate
from .schemas import SnapshottedDimension, DimensionTransitions
from spark_joinery.collection import Collection
from spark_joinery.dependencies import Context
from spark_joinery import Strict


snapshot_diff = Collection()


@snapshot_diff.transform
def read_snapshot(
    spark: SparkSession,
    path: Annotated[SnapshottedDimensionPath, Context()],
    run_date: Annotated[RunDate, Context()],
) -> Annotated[DataFrame, Strict(SnapshottedDimension)]:
    yesterdays_date = run_date - timedelta(days=1)
    return spark.read.parquet(path).filter(
        (F.col("snapshot_date") == yesterdays_date)
        | (F.col("snapshot_date") == run_date)
    )


@snapshot_diff.transform
def get_current_snapshot(
    spark: SparkSession,
    path: Annotated[SnapshottedDimensionPath, Context()],
    run_date: Annotated[RunDate, Context()],
) -> Annotated[DataFrame, Strict(SnapshottedDimension)]:
    return spark.read.parquet(path).filter(F.col("snapshot_date") == run_date)


@snapshot_diff.transform
def get_previous_snapshot(
    spark: SparkSession,
    path: Annotated[SnapshottedDimensionPath, Context()],
    run_date: Annotated[RunDate, Context()],
) -> Annotated[DataFrame, Strict(SnapshottedDimension)]:
    yesterdays_date = run_date - timedelta(days=1)
    return spark.read.parquet(path).filter(F.col("snapshot_date") == yesterdays_date)


@snapshot_diff.transform
def compute_dimension_transitions(
    previous_snapshot: Annotated[DataFrame, Strict(SnapshottedDimension)],
    current_snapshot: Annotated[DataFrame, Strict(SnapshottedDimension)],
) -> Annotated[DataFrame, Strict(DimensionTransitions)]:
    window = Window.partitionBy("entity_id").orderBy("snapshot_date")
    with_previous = previous_snapshot.union(current_snapshot).select(
        "snapshot_date",
        "entity_id",
        "dimension_field1",
        "dimension_field2",
        "dimension_updated_at",
        F.lag("dimension_field1").over(window).alias("old_dimension_field1"),
        F.lag("dimension_field2").over(window).alias("old_dimension_field2"),
    )
    # only keep rows where a previous snapshot exists to diff against
    return with_previous.filter(F.col("old_dimension_field1").isNotNull()).select(
        F.col("snapshot_date"),
        F.col("entity_id"),
        F.col("old_dimension_field1"),
        F.col("old_dimension_field2"),
        F.col("dimension_field1").alias("new_dimension_field1"),
        F.col("dimension_field2").alias("new_dimension_field2"),
        F.col("dimension_updated_at"),
    )


@snapshot_diff.transform
def write_output(
    order_with_customer_dimension: Annotated[DataFrame, Strict(DimensionTransitions)],
    path: Annotated[OutputPath, Context()],
) -> None:
    order_with_customer_dimension.write.mode("overwrite").parquet(path)
