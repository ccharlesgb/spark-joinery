from dataclasses import dataclass
from typing import Annotated, Generator

import pytest
from pyspark.sql import DataFrame, SparkSession

from spark_joinery import (
    Context as ExportedContext,
    PipelineContext as ExportedPipelineContext,
    Project,
)
from spark_joinery import transform
from spark_joinery.dependencies import Context, PipelineContext
from spark_joinery.pipeline import (
    Pipeline,
    PipelineConnectionError,
    PipelineCycleError,
    PipelineExecutionError,
)


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


@pytest.fixture(scope="session")
def spark() -> Generator[SparkSession, None, None]:
    spark = (
        SparkSession.builder.appName("pyspark-schemas-pipeline-tests")
        .master("local[*]")
        .getOrCreate()
    )
    yield spark
    spark.stop()


def test_duplicate_edges_are_idempotent():
    @transform
    def read_users(spark: SparkSession) -> Annotated[DataFrame, Project(User)]:
        return spark.createDataFrame([(1,)], "user_id INT")

    @transform
    def filter_users(
        users: Annotated[DataFrame, Project(User)],
    ) -> Annotated[DataFrame, Project(User)]:
        return users

    pipeline = Pipeline()
    users = pipeline.add_step(read_users, "users")
    filtered = pipeline.add_step(filter_users, "filtered")
    pipeline.connect(users, filtered)
    pipeline.connect(users, filtered)

    # pipeline.validate()


def test_step_right_shift_connects_steps_and_returns_downstream():
    @transform
    def read_users(spark: SparkSession) -> Annotated[DataFrame, Project(User)]:
        return spark.createDataFrame([(1,)], "user_id INT")

    @transform
    def filter_users(
        users: Annotated[DataFrame, Project(User)],
    ) -> Annotated[DataFrame, Project(User)]:
        return users

    pipeline = Pipeline()
    users = pipeline.add_step(read_users, "users")
    filtered = pipeline.add_step(filter_users, "filtered")

    assert users >> filtered is filtered
    assert pipeline.get_upstream_steps(filtered) == {users}


def test_step_right_shift_rejects_non_step_operand():
    @transform
    def read_users(spark: SparkSession) -> Annotated[DataFrame, Project(User)]:
        return spark.createDataFrame([(1,)], "user_id INT")

    pipeline = Pipeline()
    users = pipeline.add_step(read_users, "users")

    with pytest.raises(TypeError, match="only connect Step instances"):
        users >> "filtered"  # type: ignore[operator]


def test_connect_many_rejects_empty_sources():
    @transform
    def read_users(spark: SparkSession) -> Annotated[DataFrame, Project(User)]:
        return spark.createDataFrame([(1,)], "user_id INT")

    pipeline = Pipeline()
    users = pipeline.add_step(read_users, "users")

    with pytest.raises(ValueError, match="at least one"):
        pipeline.connect_many([], users)


def test_pipeline_rejects_unsupported_non_dataframe_parameter():
    @transform
    def read_users(
        spark: SparkSession, path: str
    ) -> Annotated[DataFrame, Project(User)]:
        return spark.createDataFrame([(1,)], "user_id INT")

    with pytest.raises(
        TypeError, match="only support SparkSession, annotated DataFrame"
    ):
        Pipeline().add_step(read_users, "users")


def test_pipeline_requires_spark_session_for_source():
    @transform
    def create_users() -> Annotated[DataFrame, Project(User)]:
        raise AssertionError

    with pytest.raises(TypeError, match="source step.*SparkSession"):
        Pipeline().add_step(create_users, "users")


def test_pipeline_allows_write_step_without_output_schema(spark: SparkSession):
    @transform
    def read_users(spark: SparkSession) -> Annotated[DataFrame, Project(User)]:
        return spark.createDataFrame([(1,)], "user_id BIGINT")

    written: list[DataFrame] = []

    @transform
    def write_users(users: Annotated[DataFrame, Project(User)]):
        written.append(users)

    pipeline = Pipeline()
    read_step = pipeline.add_step(read_users, "users")
    write_step = pipeline.add_step(write_users, "write_users")
    pipeline.connect(read_step, write_step)

    outputs = pipeline.run(spark)

    assert len(written) == 1
    assert "write_users" not in outputs


def test_pipeline_rejects_missing_dataframe_match():
    @transform
    def read_users(spark: SparkSession) -> Annotated[DataFrame, Project(User)]:
        return spark.createDataFrame([(1,)], "user_id INT")

    @transform
    def join_departments(
        departments: Annotated[DataFrame, Project(Department)],
    ) -> Annotated[DataFrame, Project(Department)]:
        return departments

    pipeline = Pipeline()
    users = pipeline.add_step(read_users, "users")
    join = pipeline.add_step(join_departments, "join")
    with pytest.raises(
        PipelineConnectionError,
        match="No compatible contract between steps 'users' and 'join'",
    ):
        pipeline.connect(users, join)


def test_pipeline_rejects_extra_upstream_output():
    @transform
    def read_users(spark: SparkSession) -> Annotated[DataFrame, Project(User)]:
        return spark.createDataFrame([(1,)], "user_id INT")

    @transform
    def read_departments(
        spark: SparkSession,
    ) -> Annotated[DataFrame, Project(Department)]:
        return spark.createDataFrame([(1,)], "department_id INT")

    @transform
    def accept_users(
        users: Annotated[DataFrame, Project(User)],
    ) -> Annotated[DataFrame, Project(User)]:
        return users

    pipeline = Pipeline()
    users = pipeline.add_step(read_users, "users")
    departments = pipeline.add_step(read_departments, "departments")
    accepted = pipeline.add_step(accept_users, "accepted")
    with pytest.raises(
        PipelineConnectionError,
        match="No compatible contract between steps 'departments' and 'accepted'",
    ):
        pipeline.connect_many([users, departments], accepted)


def test_pipeline_rejects_ambiguous_duplicate_schema_outputs():
    @transform
    def read_users(spark: SparkSession) -> Annotated[DataFrame, Project(User)]:
        return spark.createDataFrame([(1,)], "user_id INT")

    @transform
    def accept_users(
        users: Annotated[DataFrame, Project(User)],
    ) -> Annotated[DataFrame, Project(User)]:
        return users

    pipeline = Pipeline()
    users_a = pipeline.add_step(read_users, "users_a")
    users_b = pipeline.add_step(read_users, "users_b")
    accepted = pipeline.add_step(accept_users, "accepted")
    with pytest.raises(
        PipelineConnectionError,
        match="Step 'users_a' is already connected to 'accepted'",
    ):
        pipeline.connect_many([users_a, users_b], accepted)


def test_pipeline_rejects_cycles():
    @transform
    def first(
        users: Annotated[DataFrame, Project(User)],
    ) -> Annotated[DataFrame, Project(User)]:
        return users

    @transform
    def second(
        users: Annotated[DataFrame, Project(User)],
    ) -> Annotated[DataFrame, Project(User)]:
        return users

    pipeline = Pipeline()
    first_step = pipeline.add_step(first, "first")
    second_step = pipeline.add_step(second, "second")
    pipeline.connect(first_step, second_step)
    with pytest.raises(PipelineCycleError):
        pipeline.connect(second_step, first_step)


def test_pipeline_supports_multiple_dataframe_inputs():
    @transform
    def read_users(spark: SparkSession) -> Annotated[DataFrame, Project(User)]:
        return spark.createDataFrame([(1,)], "user_id INT")

    @transform
    def read_departments(
        spark: SparkSession,
    ) -> Annotated[DataFrame, Project(Department)]:
        return spark.createDataFrame([(1,)], "department_id INT")

    @transform
    def join(
        users: Annotated[DataFrame, Project(User)],
        departments: Annotated[DataFrame, Project(Department)],
    ) -> Annotated[DataFrame, Project(UserWithDepartment)]:
        return users.join(departments)

    pipeline = Pipeline()
    users = pipeline.add_step(read_users, "users")
    departments = pipeline.add_step(read_departments, "departments")
    joined = pipeline.add_step(join, "joined")
    pipeline.connect_many([users, departments], joined)


def test_executable_pipeline_runs_sources_and_downstream_steps(
    spark: SparkSession,
):
    @transform
    def read_users(session: SparkSession) -> Annotated[DataFrame, Project(User)]:
        return session.createDataFrame([(1,)], "user_id BIGINT")

    @transform
    def filter_users(
        users: Annotated[DataFrame, Project(User)],
    ) -> Annotated[DataFrame, Project(User)]:
        return users.filter("user_id = 1")

    pipeline = Pipeline()
    users = pipeline.add_step(read_users, "users")
    filtered = pipeline.add_step(filter_users, "filtered")
    pipeline.connect(users, filtered)

    outputs = pipeline.run(spark)

    assert set(outputs) == {"users", "filtered"}
    assert outputs["filtered"].collect()[0].user_id == 1


def test_executable_pipeline_runs_fan_in_and_independent_components(
    spark: SparkSession,
):
    @transform
    def read_users(spark: SparkSession) -> Annotated[DataFrame, Project(User)]:
        return spark.createDataFrame([(1,)], "user_id BIGINT")

    @transform
    def read_departments(
        spark: SparkSession,
    ) -> Annotated[DataFrame, Project(Department)]:
        return spark.createDataFrame([(2,)], "department_id BIGINT")

    @transform
    def join(
        users: Annotated[DataFrame, Project(User)],
        departments: Annotated[DataFrame, Project(Department)],
    ) -> Annotated[DataFrame, Project(UserWithDepartment)]:
        return users.selectExpr("user_id", "cast(2 as BIGINT) as department_id")

    pipeline = Pipeline()
    users = pipeline.add_step(read_users, "users")
    departments = pipeline.add_step(read_departments, "departments")
    joined = pipeline.add_step(join, "joined")
    pipeline.connect_many([users, departments], joined)

    outputs = pipeline.run(spark)

    assert set(outputs) == {"users", "departments", "joined"}
    assert outputs["joined"].collect()[0].user_id == 1


def test_executable_pipeline_rejects_invalid_spark_session():
    @transform
    def read_users(spark: SparkSession) -> Annotated[DataFrame, Project(User)]:
        raise AssertionError

    pipeline = Pipeline()
    pipeline.add_step(read_users, "users")

    with pytest.raises(TypeError, match="requires a SparkSession"):
        pipeline.run(object())  # type: ignore[arg-type]


def test_executable_pipeline_rejects_root_step_requiring_dataframe(
    spark: SparkSession,
):
    @transform
    def filter_users(
        users: Annotated[DataFrame, Project(User)],
    ) -> Annotated[DataFrame, Project(User)]:
        raise AssertionError("root step should not be called")

    pipeline = Pipeline()
    pipeline.add_step(filter_users, "filtered")

    with pytest.raises(
        PipelineExecutionError,
        match="first steps should be read steps that produce a dataframe",
    ):
        pipeline.run(spark)


def test_executable_pipeline_wraps_transform_failure(
    spark: SparkSession,
):
    @transform
    def read_users(spark: SparkSession) -> Annotated[DataFrame, Project(User)]:
        raise ValueError("source failed")

    pipeline = Pipeline()
    pipeline.add_step(read_users, "users")

    with pytest.raises(PipelineExecutionError, match="users") as error:
        pipeline.run(spark)

    assert isinstance(error.value.__cause__, ValueError)


def test_pipeline_accepts_context_annotated_parameter():

    @transform
    def read_users(
        spark: SparkSession, path: Annotated[PathConfig, Context()]
    ) -> Annotated[DataFrame, Project(User)]:
        return spark.createDataFrame([(1,)], "user_id INT")

    pipeline = Pipeline()
    pipeline.add_step(read_users, "users")  # must not raise


def test_run_resolves_context_parameter_from_pipeline_context(spark: SparkSession):
    @transform
    def read_users(
        spark: SparkSession, path: Annotated[PathConfig, Context()]
    ) -> Annotated[DataFrame, Project(User)]:
        assert path == PathConfig("gs://bucket/users")
        return spark.createDataFrame([(1,)], "user_id BIGINT")

    pipeline = Pipeline()
    pipeline.add_step(read_users, "users")

    context = PipelineContext(values=[PathConfig("gs://bucket/users")])
    outputs = pipeline.run(spark, context)

    assert outputs["users"].count() == 1


def test_run_raises_pipeline_execution_error_when_context_missing(spark: SparkSession):
    @transform
    def read_users(
        spark: SparkSession, path: Annotated[PathConfig, Context()]
    ) -> Annotated[DataFrame, Project(User)]:
        raise AssertionError

    pipeline = Pipeline()
    pipeline.add_step(read_users, "users")

    with pytest.raises(PipelineExecutionError, match="requires a PipelineContext"):
        pipeline.run(spark)


def test_run_raises_pipeline_execution_error_when_dependency_unregistered(
    spark: SparkSession,
):
    @transform
    def read_users(
        spark: SparkSession, path: Annotated[PathConfig, Context()]
    ) -> Annotated[DataFrame, Project(User)]:
        raise AssertionError

    pipeline = Pipeline()
    pipeline.add_step(read_users, "users")

    with pytest.raises(PipelineExecutionError, match="failed to resolve dependency"):
        pipeline.run(spark, PipelineContext(values=[]))


def test_dependency_types_are_exported_from_package():
    assert ExportedContext is Context
    assert ExportedPipelineContext is PipelineContext
