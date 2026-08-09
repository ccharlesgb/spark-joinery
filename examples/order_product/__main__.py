from pyspark.sql import SparkSession

from .pipeline import build_pipeline
from pyspark_schemas.dependencies import PipelineContext
from .context import OrdersPath, CustomersPath, OutputPath, RunDate
from .pipeline import DATA_DIR, OUTPUT_DIR


spark = (
    SparkSession.builder.appName("order-product-pipeline")
    .master("local[*]")
    .getOrCreate()
)

pipeline = build_pipeline()
context = PipelineContext(
    values=[
        OrdersPath(str(DATA_DIR / "orders.parquet")),
        CustomersPath(str(DATA_DIR / "customers.parquet")),
        OutputPath(str(OUTPUT_DIR / "output.parquet")),
        RunDate(2026, 1, 1),
    ]
)
outputs = pipeline.run(spark, context)
outputs["joined"].show()

spark.stop()
