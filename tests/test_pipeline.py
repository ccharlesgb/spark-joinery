from dataclasses import dataclass
from typing import Annotated, Generator

import pytest
from pyspark.sql import DataFrame, SparkSession

from pyspark_schemas import (
    Context as ExportedContext,
    ExecutablePipeline,
    Pipeline as ExportedPipeline,
    PipelineContext as ExportedPipelineContext,
    PipelineExecutionError as ExportedPipelineExecutionError,
    Step as ExportedStep,
)
from pyspark_schemas.collection import Collection
from pyspark_schemas.dependencies import Context, PipelineContext
from pyspark_schemas.pipeline import Pipeline, PipelineExecutionError


@dataclass
class User:
    user_id: int


@dataclass
class Department:
    department_id: int


@dataclass
class UserWithDepartment:
    user_id: int
    department_id: int


@dataclass(frozen=True)
class PathConfig:
    value: str


def test_pipeline_types_are_exported_from_package():
    assert ExportedPipeline is Pipeline
    assert ExportedPipelineExecutionError is PipelineExecutionError
    assert ExecutablePipeline.__name__ == "ExecutablePipeline"
    assert ExportedStep.__name__ == "Step"


@pytest.fixture(scope="session")
def spark() -> Generator[SparkSession, None, None]:
    spark = (
        SparkSession.builder.appName("pyspark-schemas-pipeline-tests")
        .master("local[*]")
        .getOrCreate()
    )
    yield spark
    spark.stop()


def test_validate_returns_cached_executable_and_freezes_pipeline():
    collection = Collection()

    @collection.transform
    def read_users(spark: SparkSession) -> Annotated[DataFrame, User]:
        return spark.createDataFrame([(1,)], "user_id INT")

    pipeline = Pipeline(collections=[collection])
    pipeline.add_step(read_users, "users")

    executable = pipeline.validate()

    assert executable is pipeline.validate()
    with pytest.raises(RuntimeError, match="validated"):
        pipeline.add_step(read_users, "another_users")


def test_duplicate_edges_are_idempotent():
    collection = Collection()

    @collection.transform
    def read_users(spark: SparkSession) -> Annotated[DataFrame, User]:
        return spark.createDataFrame([(1,)], "user_id INT")

    @collection.transform
    def filter_users(
        users: Annotated[DataFrame, User],
    ) -> Annotated[DataFrame, User]:
        return users

    pipeline = Pipeline(collections=[collection])
    users = pipeline.add_step(read_users, "users")
    filtered = pipeline.add_step(filter_users, "filtered")
    pipeline.connect(users, filtered)
    pipeline.connect(users, filtered)

    pipeline.validate()


def test_connect_rejects_steps_from_different_pipelines():
    collection = Collection()

    @collection.transform
    def read_users(spark: SparkSession) -> Annotated[DataFrame, User]:
        return spark.createDataFrame([(1,)], "user_id INT")

    first = Pipeline(collections=[collection])
    second = Pipeline(collections=[collection])
    users = first.add_step(read_users, "users")
    filtered = second.add_step(read_users, "filtered")

    with pytest.raises(ValueError, match="same pipeline"):
        first.connect(users, filtered)


def test_connect_many_rejects_empty_sources():
    collection = Collection()

    @collection.transform
    def read_users(spark: SparkSession) -> Annotated[DataFrame, User]:
        return spark.createDataFrame([(1,)], "user_id INT")

    pipeline = Pipeline(collections=[collection])
    users = pipeline.add_step(read_users, "users")

    with pytest.raises(ValueError, match="at least one"):
        pipeline.connect_many([], users)


def test_pipeline_rejects_unsupported_non_dataframe_parameter():
    collection = Collection()

    @collection.transform
    def read_users(spark: SparkSession, path: str) -> Annotated[DataFrame, User]:
        return spark.createDataFrame([(1,)], "user_id INT")

    with pytest.raises(
        TypeError, match="only support SparkSession, annotated DataFrame"
    ):
        Pipeline(collections=[collection]).add_step(read_users, "users")


def test_pipeline_requires_spark_session_for_source():
    collection = Collection()

    @collection.transform
    def create_users() -> Annotated[DataFrame, User]:
        raise AssertionError

    with pytest.raises(TypeError, match="source step.*SparkSession"):
        Pipeline(collections=[collection]).add_step(create_users, "users")


def test_pipeline_allows_write_step_without_output_schema(spark: SparkSession):
    collection = Collection()

    @collection.transform
    def read_users(spark: SparkSession) -> Annotated[DataFrame, User]:
        return spark.createDataFrame([(1,)], "user_id INT")

    written: list[DataFrame] = []

    @collection.transform
    def write_users(users: Annotated[DataFrame, User]):
        written.append(users)

    pipeline = Pipeline(collections=[collection])
    read_step = pipeline.add_step(read_users, "users")
    write_step = pipeline.add_step(write_users, "write_users")
    pipeline.connect(read_step, write_step)

    outputs = pipeline.validate().run(spark)

    assert len(written) == 1
    assert "write_users" not in outputs


def test_pipeline_rejects_missing_dataframe_match():
    collection = Collection()

    @collection.transform
    def read_users(spark: SparkSession) -> Annotated[DataFrame, User]:
        return spark.createDataFrame([(1,)], "user_id INT")

    @collection.transform
    def join_departments(
        departments: Annotated[DataFrame, Department],
    ) -> Annotated[DataFrame, Department]:
        return departments

    pipeline = Pipeline(collections=[collection])
    users = pipeline.add_step(read_users, "users")
    join = pipeline.add_step(join_departments, "join")
    pipeline.connect(users, join)

    with pytest.raises(ValueError, match="no upstream step provides"):
        pipeline.validate()


def test_pipeline_rejects_extra_upstream_output():
    collection = Collection()

    @collection.transform
    def read_users(spark: SparkSession) -> Annotated[DataFrame, User]:
        return spark.createDataFrame([(1,)], "user_id INT")

    @collection.transform
    def read_departments(spark: SparkSession) -> Annotated[DataFrame, Department]:
        return spark.createDataFrame([(1,)], "department_id INT")

    @collection.transform
    def accept_users(users: Annotated[DataFrame, User]) -> Annotated[DataFrame, User]:
        return users

    pipeline = Pipeline(collections=[collection])
    users = pipeline.add_step(read_users, "users")
    departments = pipeline.add_step(read_departments, "departments")
    accepted = pipeline.add_step(accept_users, "accepted")
    pipeline.connect_many([users, departments], accepted)

    with pytest.raises(ValueError, match="does not match a parameter"):
        pipeline.validate()


def test_pipeline_rejects_ambiguous_duplicate_schema_outputs():
    collection = Collection()

    @collection.transform
    def read_users(spark: SparkSession) -> Annotated[DataFrame, User]:
        return spark.createDataFrame([(1,)], "user_id INT")

    @collection.transform
    def accept_users(users: Annotated[DataFrame, User]) -> Annotated[DataFrame, User]:
        return users

    pipeline = Pipeline(collections=[collection])
    users_a = pipeline.add_step(read_users, "users_a")
    users_b = pipeline.add_step(read_users, "users_b")
    accepted = pipeline.add_step(accept_users, "accepted")
    pipeline.connect_many([users_a, users_b], accepted)

    with pytest.raises(ValueError, match="ambiguous"):
        pipeline.validate()


def test_pipeline_rejects_cycles():
    collection = Collection()

    @collection.transform
    def first(users: Annotated[DataFrame, User]) -> Annotated[DataFrame, User]:
        return users

    @collection.transform
    def second(users: Annotated[DataFrame, User]) -> Annotated[DataFrame, User]:
        return users

    pipeline = Pipeline(collections=[collection])
    first_step = pipeline.add_step(first, "first")
    second_step = pipeline.add_step(second, "second")
    pipeline.connect(first_step, second_step)
    pipeline.connect(second_step, first_step)

    with pytest.raises(ValueError, match="cycle"):
        pipeline.validate()


def test_pipeline_supports_multiple_dataframe_inputs():
    collection = Collection()

    @collection.transform
    def read_users(spark: SparkSession) -> Annotated[DataFrame, User]:
        return spark.createDataFrame([(1,)], "user_id INT")

    @collection.transform
    def read_departments(spark: SparkSession) -> Annotated[DataFrame, Department]:
        return spark.createDataFrame([(1,)], "department_id INT")

    @collection.transform
    def join(
        users: Annotated[DataFrame, User],
        departments: Annotated[DataFrame, Department],
    ) -> Annotated[DataFrame, UserWithDepartment]:
        return users.join(departments)

    pipeline = Pipeline(collections=[collection])
    users = pipeline.add_step(read_users, "users")
    departments = pipeline.add_step(read_departments, "departments")
    joined = pipeline.add_step(join, "joined")
    pipeline.connect_many([users, departments], joined)

    pipeline.validate()


def test_executable_pipeline_runs_sources_and_downstream_steps(
    spark: SparkSession,
):
    collection = Collection()

    @collection.transform
    def read_users(session: SparkSession) -> Annotated[DataFrame, User]:
        return session.createDataFrame([(1,)], "user_id INT")

    @collection.transform
    def filter_users(
        users: Annotated[DataFrame, User],
    ) -> Annotated[DataFrame, User]:
        return users.filter("user_id = 1")

    pipeline = Pipeline(collections=[collection])
    users = pipeline.add_step(read_users, "users")
    filtered = pipeline.add_step(filter_users, "filtered")
    pipeline.connect(users, filtered)

    outputs = pipeline.validate().run(spark)

    assert set(outputs) == {"users", "filtered"}
    assert outputs["filtered"].collect()[0].user_id == 1


def test_executable_pipeline_runs_fan_in_and_independent_components(
    spark: SparkSession,
):
    collection = Collection()

    @collection.transform
    def read_users(spark: SparkSession) -> Annotated[DataFrame, User]:
        return spark.createDataFrame([(1,)], "user_id INT")

    @collection.transform
    def read_departments(spark: SparkSession) -> Annotated[DataFrame, Department]:
        return spark.createDataFrame([(2,)], "department_id INT")

    @collection.transform
    def join(
        users: Annotated[DataFrame, User],
        departments: Annotated[DataFrame, Department],
    ) -> Annotated[DataFrame, UserWithDepartment]:
        return users.selectExpr("user_id", "2 as department_id")

    pipeline = Pipeline(collections=[collection])
    users = pipeline.add_step(read_users, "users")
    departments = pipeline.add_step(read_departments, "departments")
    joined = pipeline.add_step(join, "joined")
    pipeline.connect_many([users, departments], joined)

    outputs = pipeline.validate().run(spark)

    assert set(outputs) == {"users", "departments", "joined"}
    assert outputs["joined"].collect()[0].user_id == 1


def test_executable_pipeline_rejects_invalid_spark_session():
    collection = Collection()

    @collection.transform
    def read_users(spark: SparkSession) -> Annotated[DataFrame, User]:
        raise AssertionError

    pipeline = Pipeline(collections=[collection])
    pipeline.add_step(read_users, "users")
    executable = pipeline.validate()

    with pytest.raises(TypeError, match="requires a SparkSession"):
        executable.run(object())  # type: ignore[arg-type]


def test_executable_pipeline_wraps_transform_failure(
    spark: SparkSession,
):
    collection = Collection()

    @collection.transform
    def read_users(spark: SparkSession) -> Annotated[DataFrame, User]:
        raise ValueError("source failed")

    pipeline = Pipeline(collections=[collection])
    pipeline.add_step(read_users, "users")

    with pytest.raises(PipelineExecutionError, match="users") as error:
        pipeline.validate().run(spark)

    assert isinstance(error.value.__cause__, ValueError)


def test_pipeline_accepts_context_annotated_parameter():
    collection = Collection()

    @collection.transform
    def read_users(
        spark: SparkSession, path: Annotated[PathConfig, Context()]
    ) -> Annotated[DataFrame, User]:
        return spark.createDataFrame([(1,)], "user_id INT")

    pipeline = Pipeline(collections=[collection])
    pipeline.add_step(read_users, "users")  # must not raise


def test_run_resolves_context_parameter_from_pipeline_context(spark: SparkSession):
    collection = Collection()

    @collection.transform
    def read_users(
        spark: SparkSession, path: Annotated[PathConfig, Context()]
    ) -> Annotated[DataFrame, User]:
        assert path == PathConfig("gs://bucket/users")
        return spark.createDataFrame([(1,)], "user_id INT")

    pipeline = Pipeline(collections=[collection])
    pipeline.add_step(read_users, "users")
    executable = pipeline.validate()

    context = PipelineContext(values=[PathConfig("gs://bucket/users")])
    outputs = executable.run(spark, context)

    assert outputs["users"].count() == 1


def test_run_raises_pipeline_execution_error_when_context_missing(spark: SparkSession):
    collection = Collection()

    @collection.transform
    def read_users(
        spark: SparkSession, path: Annotated[PathConfig, Context()]
    ) -> Annotated[DataFrame, User]:
        raise AssertionError

    pipeline = Pipeline(collections=[collection])
    pipeline.add_step(read_users, "users")
    executable = pipeline.validate()

    with pytest.raises(PipelineExecutionError, match="requires a PipelineContext"):
        executable.run(spark)


def test_run_raises_pipeline_execution_error_when_dependency_unregistered(
    spark: SparkSession,
):
    collection = Collection()

    @collection.transform
    def read_users(
        spark: SparkSession, path: Annotated[PathConfig, Context()]
    ) -> Annotated[DataFrame, User]:
        raise AssertionError

    pipeline = Pipeline(collections=[collection])
    pipeline.add_step(read_users, "users")
    executable = pipeline.validate()

    with pytest.raises(PipelineExecutionError, match="failed to resolve dependency"):
        executable.run(spark, PipelineContext(values=[]))


def test_dependency_types_are_exported_from_package():
    assert ExportedContext is Context
    assert ExportedPipelineContext is PipelineContext
