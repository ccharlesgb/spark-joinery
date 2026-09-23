from pathlib import Path

from .collection import (
    read_customers,
    denormalise_group_id,
    write_output,
)
from spark_joinery.pipeline import Pipeline, ExecutablePipeline


DATA_DIR = Path(__file__).parent / "data"
OUTPUT_DIR = Path(__file__).parent / "__output"


def build_pipeline() -> ExecutablePipeline:
    pipeline = Pipeline()

    read_customers_step = pipeline.add_step(read_customers, "read_customers")
    denormalise_group_id_step = pipeline.add_step(
        denormalise_group_id, "denormalise_group_id"
    )
    write_output_step = pipeline.add_step(write_output, "write_output")

    pipeline.connect(read_customers_step, denormalise_group_id_step)
    pipeline.connect(denormalise_group_id_step, write_output_step)

    return pipeline.validate()
