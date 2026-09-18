from dataclasses import dataclass

import pytest

from spark_joinery.dependencies import Context, PipelineContext


@dataclass(frozen=True)
class PathConfig:
    value: str


@dataclass(frozen=True)
class OtherConfig:
    value: str


def test_pipeline_context_resolves_value_by_annotated_type():
    context = PipelineContext(values=[PathConfig("gs://bucket/path")])

    resolved = context.resolve(PathConfig, Context())

    assert resolved == PathConfig("gs://bucket/path")


def test_pipeline_context_resolves_by_explicit_context_type_override():
    context = PipelineContext(values=[PathConfig("gs://bucket/path")])

    resolved = context.resolve(OtherConfig, Context(PathConfig))

    assert resolved == PathConfig("gs://bucket/path")


def test_pipeline_context_raises_key_error_for_unregistered_type():
    context = PipelineContext(values=[PathConfig("gs://bucket/path")])

    with pytest.raises(KeyError, match="OtherConfig"):
        context.resolve(OtherConfig, Context())


def test_pipeline_context_rejects_duplicate_types_at_construction():
    with pytest.raises(ValueError, match="duplicate dependency type"):
        PipelineContext(values=[PathConfig("a"), PathConfig("b")])
