from pathlib import Path


from spark_joinery.pipeline import Pipeline, ExecutablePipeline

from .collection import (
    get_current_snapshot,
    get_previous_snapshot,
    compute_dimension_transitions,
    write_output,
)

DATA_DIR = Path(__file__).parent / "data"
OUTPUT_DIR = Path(__file__).parent / "__output"


def build_pipeline() -> ExecutablePipeline:
    pipeline = Pipeline()
    previous_snapshot = pipeline.add_step(
        get_previous_snapshot, "get_previous_snapshot"
    )
    current_snapshot = pipeline.add_step(get_current_snapshot, "get_current_snapshot")
    dimension_transitions = pipeline.add_step(
        compute_dimension_transitions, "compute_dimension_transitions"
    )
    output = pipeline.add_step(write_output, "write_output")
    pipeline.connect(
        previous_snapshot, dimension_transitions, param="previous_snapshot"
    )
    pipeline.connect(current_snapshot, dimension_transitions, param="current_snapshot")
    pipeline.connect(dimension_transitions, output)

    return pipeline.validate()
