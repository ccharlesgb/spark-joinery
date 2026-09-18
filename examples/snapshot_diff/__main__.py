from pyspark.sql import SparkSession

from spark_joinery.dependencies import PipelineContext
from .context import OutputPath, SnapshottedDimensionPath, RunDate
from .pipeline import build_pipeline, DATA_DIR, OUTPUT_DIR


spark = (
    SparkSession.builder.appName("snapshot-diff-pipeline")
    .master("local[*]")
    .getOrCreate()
)

pipeline = build_pipeline()
context = PipelineContext(
    values=[
        SnapshottedDimensionPath(str(DATA_DIR / "snapshotted_dimension.parquet")),
        OutputPath(str(OUTPUT_DIR / "output.parquet")),
        RunDate(2026, 1, 1),
    ]
)
outputs = pipeline.run(spark, context)
outputs["compute_dimension_transitions"].show()

spark.stop()
