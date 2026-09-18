from __future__ import annotations

import inspect
from dataclasses import dataclass
from typing import Any, Callable, ParamSpec, Sequence, TypeVar, overload

from pyspark.sql import DataFrame, SparkSession, types

from spark_joinery.utils import get_callable_name

from .collection import Collection
from .dependencies import Context, PipelineContext
from .schemas import CoercionMode

Transform = Callable[..., DataFrame | None]
P = ParamSpec("P")
R = TypeVar("R")


class PipelineExecutionError(RuntimeError):
    pass


@dataclass(eq=False)
class Step:
    name: str
    transform: Transform
    _pipeline: Pipeline
    _input_schemas: dict[str, types.StructType]
    _output_schema: types.StructType | None
    _spark_parameter: str | None
    _context_parameters: dict[str, tuple[type, Context]]
    _upstream_steps: list[Step]
    _explicit_bindings: dict[Step, str]


@dataclass(frozen=True)
class _ExecutionStep:
    step: Step
    dataframe_bindings: tuple[tuple[str, Step], ...]


class ExecutablePipeline:
    def __init__(self, execution_steps: Sequence[_ExecutionStep]):
        self._execution_steps = tuple(execution_steps)

    def run(
        self, spark: SparkSession, context: PipelineContext | None = None
    ) -> dict[str, DataFrame]:
        if not isinstance(spark, SparkSession):
            raise TypeError("run() requires a SparkSession")

        outputs: dict[str, DataFrame] = {}
        for execution_step in self._execution_steps:
            step = execution_step.step
            arguments: dict[str, Any] = {
                parameter_name: outputs[upstream_step.name]
                for parameter_name, upstream_step in execution_step.dataframe_bindings
            }
            if step._spark_parameter is not None:
                arguments[step._spark_parameter] = spark

            for parameter_name, (
                param_type,
                marker,
            ) in step._context_parameters.items():
                if context is None:
                    raise PipelineExecutionError(
                        f"step '{step.name}' requires a PipelineContext but none was provided"
                    )
                try:
                    arguments[parameter_name] = context.resolve(param_type, marker)
                except KeyError as error:
                    raise PipelineExecutionError(
                        f"step '{step.name}' failed to resolve dependency"
                    ) from error

            try:
                result = step.transform(**arguments)
                if step._output_schema is not None and not isinstance(
                    result, DataFrame
                ):
                    raise TypeError(
                        f"step '{step.name}' returned a non-DataFrame value"
                    )
            except Exception as error:
                if isinstance(error, PipelineExecutionError):
                    raise
                raise PipelineExecutionError(
                    f"Pipeline step '{step.name}' failed"
                ) from error

            if isinstance(result, DataFrame):
                outputs[step.name] = result

        return outputs


class Pipeline:
    def __init__(self, collections: Sequence[Collection] = ()):
        self._collection = Collection()
        self._collections: tuple[Collection, ...] = (self._collection, *collections)
        self._steps: dict[str, Step] = {}
        self._validated = False
        self._executable: ExecutablePipeline | None = None

    @overload
    def transform(
        self,
        f: Callable[P, R],
        *,
        validate_input: CoercionMode | None = "project_all",
        validate_output: CoercionMode | None = "project_all",
    ) -> Callable[P, R]: ...

    @overload
    def transform(
        self,
        f: None = None,
        *,
        validate_input: CoercionMode | None = "project_all",
        validate_output: CoercionMode | None = "project_all",
    ) -> Callable[[Callable[P, R]], Callable[P, R]]: ...

    def transform(
        self,
        f: Callable[P, R] | None = None,
        *,
        validate_input: CoercionMode | None = "project_all",
        validate_output: CoercionMode | None = "project_all",
    ) -> Callable[P, R] | Callable[[Callable[P, R]], Callable[P, R]]:
        return self._collection.transform(
            f,
            validate_input=validate_input,
            validate_output=validate_output,
        )

    def _resolve_spec(self, transform: Transform):
        for collection in self._collections:
            spec = collection._specs.get(transform)
            if spec is not None:
                return spec
        name = getattr(transform, "__name__", repr(transform))
        raise TypeError(f"'{name}' is not registered in this pipeline's collections")

    def add_step(self, transform: Transform, name: str | None = None) -> Step:
        self._ensure_mutable()
        if name is None:
            name = get_callable_name(transform)
        if name is None:
            raise ValueError(
                f"Step name of '{transform}' could not be inferred. Pass name=<desired_name>"
            )
        if name in self._steps:
            raise ValueError(f"step name '{name}' is already registered")

        spec = self._resolve_spec(transform)
        input_schemas = spec.input_schemas
        output_schema = spec.output_schema

        spark_parameter = spec.spark_parameter
        parameter_names = set(inspect.signature(transform).parameters)
        supported_parameters = (
            set(input_schemas)
            | ({spark_parameter} if spark_parameter is not None else set())
            | set(spec.context_parameters)
        )
        unsupported_parameters = parameter_names - supported_parameters
        if unsupported_parameters:
            unsupported = sorted(unsupported_parameters)[0]
            raise TypeError(
                "pipeline steps only support SparkSession, annotated DataFrame, "
                f"and Context-annotated parameters; unsupported parameter '{unsupported}'"
            )

        if not input_schemas and spark_parameter is None:
            raise TypeError("source step must declare a SparkSession")

        step = Step(
            name=name,
            transform=transform,
            _pipeline=self,
            _input_schemas=input_schemas,
            _output_schema=output_schema,
            _spark_parameter=spark_parameter,
            _context_parameters=spec.context_parameters,
            _upstream_steps=[],
            _explicit_bindings={},
        )
        self._steps[name] = step
        return step

    def connect(
        self, upstream: Step, downstream: Step, *, param: str | None = None
    ) -> None:
        self._ensure_mutable()
        self._ensure_owned(upstream)
        self._ensure_owned(downstream)
        if upstream not in downstream._upstream_steps:
            downstream._upstream_steps.append(upstream)
        if param is not None:
            for other_upstream, other_param in downstream._explicit_bindings.items():
                if other_param == param and other_upstream is not upstream:
                    raise ValueError(
                        f"step '{downstream.name}' parameter '{param}' is already "
                        f"bound to upstream step '{other_upstream.name}'"
                    )
            downstream._explicit_bindings[upstream] = param

    def connect_many(
        self,
        upstream_steps: Sequence[Step],
        downstream: Step,
    ) -> None:
        self._ensure_mutable()
        if not upstream_steps:
            raise ValueError("connect_many requires at least one upstream step")
        for upstream in upstream_steps:
            self.connect(upstream, downstream)

    def validate(self) -> ExecutablePipeline:
        if self._executable is not None:
            return self._executable

        execution_steps: list[_ExecutionStep] = []
        for step in self._topological_order():
            bindings: list[tuple[str, Step]] = []
            matched_upstream: set[Step] = set()
            for parameter_name, expected_schema in step._input_schemas.items():
                explicit_upstream = next(
                    (
                        upstream
                        for upstream, bound_param in step._explicit_bindings.items()
                        if bound_param == parameter_name
                    ),
                    None,
                )
                if explicit_upstream is not None:
                    if explicit_upstream._output_schema != expected_schema:
                        raise ValueError(
                            f"step '{step.name}' parameter '{parameter_name}' is "
                            f"explicitly bound to '{explicit_upstream.name}' but its "
                            "output schema does not match"
                        )
                    bindings.append((parameter_name, explicit_upstream))
                    matched_upstream.add(explicit_upstream)
                    continue

                candidates = [
                    upstream
                    for upstream in step._upstream_steps
                    if upstream not in step._explicit_bindings
                ]
                matches = [
                    upstream
                    for upstream in candidates
                    if upstream._output_schema == expected_schema
                ]
                if not matches:
                    raise ValueError(
                        f"step '{step.name}' parameter '{parameter_name}' has no "
                        "upstream step provides a matching schema"
                    )
                if len(matches) > 1:
                    names = ", ".join(upstream.name for upstream in matches)
                    raise ValueError(
                        f"step '{step.name}' has ambiguous upstream schema for "
                        f"parameter '{parameter_name}': {names}"
                    )
                upstream = matches[0]
                bindings.append((parameter_name, upstream))
                matched_upstream.add(upstream)

            unmatched = [
                upstream
                for upstream in step._upstream_steps
                if upstream not in matched_upstream
            ]
            if unmatched:
                names = ", ".join(upstream.name for upstream in unmatched)
                raise ValueError(
                    f"step '{step.name}' has upstream output that does not match "
                    f"a parameter: {names}"
                )

            execution_steps.append(_ExecutionStep(step, tuple(bindings)))

        self._validated = True
        self._executable = ExecutablePipeline(execution_steps)
        return self._executable

    def _ensure_mutable(self) -> None:
        if self._validated:
            raise RuntimeError("pipeline has already been validated")

    def _ensure_owned(self, step: Step) -> None:
        if step._pipeline is not self:
            raise ValueError("steps must belong to the same pipeline")

    def _topological_order(self) -> list[Step]:
        states: dict[Step, int] = {}
        ordered: list[Step] = []

        def visit(step: Step) -> None:
            state = states.get(step, 0)
            if state == 1:
                raise ValueError("pipeline contains a cycle")
            if state == 2:
                return
            states[step] = 1
            for upstream in step._upstream_steps:
                visit(upstream)
            states[step] = 2
            ordered.append(step)

        for step in self._steps.values():
            visit(step)
        return ordered
