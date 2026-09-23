from __future__ import annotations

from dataclasses import dataclass
from typing import Any, ParamSpec, Sequence, TypeVar
from matplotlib import pyplot as plt
from pyspark.sql import DataFrame, SparkSession
import rustworkx as rx
from rustworkx.visualization import mpl_draw

from spark_joinery.dependencies import PipelineContext
from spark_joinery.transform import Contract, Transform
from spark_joinery.visualisation import topological_layout


P = ParamSpec("P")
R = TypeVar("R")


class PipelineExecutionError(RuntimeError):
    pass


class PipelineCycleError(Exception):
    pass


class PipelineConnectionError(Exception):
    pass


@dataclass(eq=False, frozen=True)
class Step:
    """
    Represents a single step in a pipeline, encapsulating a transform and its connections.
    It's purpose is to allow you to use a transformation more than once within the same pipeline.
    """

    name: str
    transform: Transform
    _pipeline: Pipeline

    def __rshift__(self, other: Step) -> Step:
        if not isinstance(other, Step):
            raise TypeError("Can only connect Step instances using >>")
        self._pipeline.connect(self, other)
        return other


class Pipeline:
    def __init__(self):
        self._dag = rx.PyDAG(check_cycle=True)
        self._node_indices: dict[Step, int] = {}

    def add_step(self, transform: Transform, name: str | None = None) -> Step:
        """Adds a new step to the pipeline.

        Args:
            transform: The transform to add as a step in the pipeline.
            name: The name of the step. If None, the name will be inferred from the transform.

        Returns:
            The newly created Step instance.

        Raises:
            ValueError: If the step name is already registered or cannot be inferred.
            TypeError: If the transform has unsupported parameters or lacks required inputs.
        """
        if name is None:
            name = transform.default_name
        if name is None:
            raise ValueError(
                f"Step name of '{transform}' could not be inferred. Pass name=<desired_name>"
            )
        if name in [node.name for node in self._dag.nodes()]:
            raise ValueError(f"step name '{name}' is already registered")

        spec = transform.__transform_spec__
        input_schemas = spec.input_contracts

        spark_parameter = spec.spark_parameter
        parameter_names = set(transform.get_signature().parameters)
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
        )
        node_index = self._dag.add_node(step)
        self._node_indices[step] = node_index
        return step

    def connect(
        self, upstream: Step, downstream: Step, *, param: str | None = None
    ) -> None:
        downstream_spec = downstream.transform.__transform_spec__
        upstream_spec = upstream.transform.__transform_spec__

        upstream_contract = upstream_spec.output_contract
        if upstream_contract is None:
            raise PipelineConnectionError(
                f"upstream step '{upstream.name}' does not produce an output"
            )

        compatible_specs: dict[str, Contract] = {}
        for param_name, input_contract in downstream_spec.input_contracts.items():
            if input_contract.is_compatible_with(upstream_contract):
                compatible_specs[param_name] = input_contract

        if len(compatible_specs) > 1:
            if param is None:
                raise PipelineConnectionError(
                    f"Multiple compatible input contracts found for upstream step '{upstream.name}' "
                    f"and downstream step '{downstream.name}', but no parameter was specified"
                )
            else:
                downstream_parameter_name = param
        elif len(compatible_specs) == 1:
            if param is not None and param != list(compatible_specs.keys())[0]:
                raise PipelineConnectionError(
                    f"Specified parameter '{param}' does not match the compatible input contract '{list(compatible_specs.keys())[0]}'"
                )
            downstream_parameter_name = list(compatible_specs.keys())[0]
        else:
            raise PipelineConnectionError(
                f"No compatible contract between steps '{upstream.name}' and '{downstream.name}'"
            )

        downstream_index = self._node_indices[downstream]
        for connected_upstream_index, _, connected_parameter_name in self._dag.in_edges(
            downstream_index
        ):
            if connected_parameter_name != downstream_parameter_name:
                continue
            if connected_upstream_index == self._node_indices[upstream]:
                return
            connected_upstream = self._dag[connected_upstream_index]
            raise PipelineConnectionError(
                f"Step '{connected_upstream.name}' is already connected to "
                f"'{downstream.name}'"
            )

        try:
            self._dag.add_edge(
                self._node_indices[upstream],
                downstream_index,
                downstream_parameter_name,
            )
        except rx.DAGWouldCycle:
            raise PipelineCycleError(
                f"Connecting upstream step '{upstream.name}' to downstream step '{downstream.name}' would create a cycle"
            )

    def connect_many(
        self,
        upstream_steps: Sequence[Step],
        downstream: Step,
    ) -> None:
        if not upstream_steps:
            raise ValueError("connect_many requires at least one upstream step")
        for upstream in upstream_steps:
            self.connect(upstream, downstream)

    def get_upstream_steps(self, step: Step) -> set[Step]:
        return set(self._dag.predecessors(self._node_indices[step]))

    def get_steps_in_execution_order(self) -> list[Step]:
        node_indices = rx.topological_sort(self._dag)
        return [self._dag[node_index] for node_index in node_indices]

    def _validate_source_steps(self) -> None:
        for step, step_index in self._node_indices.items():
            if (
                self._dag.in_degree(step_index) == 0
                and step.transform.__transform_spec__.input_contracts
            ):
                raise PipelineExecutionError(
                    f"first pipeline step '{step.name}' requires input dataframes; "
                    "first steps should be read steps that produce a dataframe"
                )

    def run(
        self, spark: SparkSession, context: PipelineContext | None = None
    ) -> dict[str, DataFrame]:
        if not isinstance(spark, SparkSession):
            raise TypeError("run() requires a SparkSession")

        self._validate_source_steps()

        outputs: dict[str, DataFrame] = {}

        for step in self.get_steps_in_execution_order():
            spec = step.transform.__transform_spec__
            arguments: dict[str, Any] = {}
            step_index = self._node_indices[step]

            for upstream_index, _, parameter_name in self._dag.in_edges(step_index):
                upstream_step = self._dag[upstream_index]
                arguments[parameter_name] = outputs[upstream_step.name]
            if spec.spark_parameter is not None:
                arguments[spec.spark_parameter] = spark

            for parameter_name, (
                param_type,
                marker,
            ) in spec.context_parameters.items():
                if context is None:
                    raise PipelineExecutionError(
                        f"step '{step.name}' requires a PipelineContext but none was provided"
                    )
                try:
                    arguments[parameter_name] = context.resolve(param_type, marker)
                except KeyError as error:
                    raise PipelineExecutionError(
                        f"step '{step.name}' failed to resolve dependency '{param_type.__name__}'"
                    ) from error

            try:
                result = step.transform(**arguments)
                if spec.output_contract is not None and not isinstance(
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

    def visualize(self):
        mpl_draw(
            self._dag,
            pos=topological_layout(self._dag),
            with_labels=True,
            labels=lambda node: node.name,
            edge_labels=lambda edge: edge,
            node_shape="s",
            node_size=500,
            font_size=8,
        )
        plt.show()
