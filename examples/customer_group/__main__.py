from pyspark.sql import SparkSession

from .context import CustomersPath, OutputPath
from .pipeline import DATA_DIR, OUTPUT_DIR, build_pipeline
from spark_joinery.dependencies import PipelineContext

spark = (
    SparkSession.builder.appName("customer-group-pipeline")
    .master("local[*]")
    .getOrCreate()
)

pipeline = build_pipeline()
context = PipelineContext(
    values=[
        CustomersPath(str(DATA_DIR / "customers.parquet")),
        OutputPath(str(OUTPUT_DIR / "output.parquet")),
    ]
)
outputs = pipeline.run(spark, context)
outputs["denormalise_group_id"].show()

spark.stop()
