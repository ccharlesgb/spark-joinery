# Pipeline Wiring Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add explicit Spark pipeline graph construction, validation, and execution around the existing schema-aware transform decorator.

**Architecture:** Keep `transform` as the source of DataFrame schema metadata and add a focused pipeline module containing mutable `Pipeline`/`Step` builders plus immutable `ExecutablePipeline` plans. Validation resolves a topological order and exact schema-to-parameter bindings before execution; runtime only executes that plan with a supplied `SparkSession`.

**Tech Stack:** Python 3.13+, PySpark, `inspect`, `dataclasses`, pytest, existing schema inspection helpers.

**Spec:** `docs/superpowers/specs/2026-09-09-pipeline-wiring-design.md`

## Global Constraints

- Pipeline steps support only `SparkSession` and annotated `DataFrame` inputs.
- Every source step must declare a `SparkSession`; every non-source step must consume an annotated DataFrame.
- Duplicate edges are idempotent no-ops.
- Independent graph components are valid.
- `validate()` freezes the builder and returns a cached executable plan.
- Only `ExecutablePipeline.run(spark)` executes the graph.
- Schema ambiguity, missing inputs, cycles, and unsupported parameters fail during validation.
- Transform failures are wrapped with the step name and preserve the original cause.
- Do not add dependency injection, operator syntax, external inputs, caching, retries, or parallel execution.

---

### Task 1: Expose reusable transform metadata

**Files:**
- Modify: `src/pyspark_schemas/transform.py`
- Test: `tests/test_transform.py`

**Interfaces:**
- Consumes: Existing `_get_annotated_input_schemas` and `_get_annotated_output_schema` helpers.
- Produces: Publicly usable metadata accessors for pipeline validation without changing existing `@transform` behavior.

- [ ] **Step 1: Write the failing metadata tests**

Add tests that decorate a function with annotated DataFrame inputs and output, then assert the metadata accessors return the expected parameter schema mapping and output schema. Add a test that an unannotated output returns `None`.

- [ ] **Step 2: Run the focused tests**

Run: `uv run pytest tests/test_transform.py -q`

Expected: FAIL because the metadata accessors do not yet exist.

- [ ] **Step 3: Implement thin metadata accessors**

Expose focused functions that delegate to the existing private inspection logic, for example:

```python
def get_transform_input_schemas(function: Any) -> dict[str, types.StructType]:
    return _get_annotated_input_schemas(function)


def get_transform_output_schema(function: Any) -> types.StructType | None:
    return _get_annotated_output_schema(function)
```

Keep schema validation and wrapper behavior unchanged.

- [ ] **Step 4: Run the focused tests**

Run: `uv run pytest tests/test_transform.py -q`

Expected: PASS.

### Task 2: Add pipeline builder and graph validation

**Files:**
- Create: `src/pyspark_schemas/pipeline.py`
- Create: `tests/test_pipeline.py`

**Interfaces:**
- Consumes: Transform metadata accessors from Task 1 and `schemas` helpers.
- Produces: `Step`, `Pipeline`, `ExecutablePipeline`, and `PipelineExecutionError` public classes.

- [ ] **Step 1: Write failing builder and validation tests**

Cover these concrete behaviors:

```python
pipeline = Pipeline()
source = pipeline.add_step(read_users, "users")
filtered = pipeline.add_step(filter_users, "filtered")
pipeline.connect(source, filtered)
executable = pipeline.validate()
assert executable is pipeline.validate()
```

Also test duplicate names, cross-pipeline edges, duplicate-edge idempotence, mutation after validation, unsupported parameters, missing SparkSession on sources, missing DataFrame inputs on non-sources, unannotated outputs, missing schema matches, extra upstream outputs, duplicate schema ambiguity, and cycles.

- [ ] **Step 2: Run the pipeline tests to verify failure**

Run: `uv run pytest tests/test_pipeline.py -q`

Expected: FAIL because the pipeline module does not exist.

- [ ] **Step 3: Implement the graph builder**

Use a private pipeline owner on each `Step`, a name-to-step mapping on `Pipeline`, and sets for downstream/upstream relationships so duplicate connections are naturally idempotent. `connect_many` should call the same edge registration logic for each source and reject an empty source sequence.

- [ ] **Step 4: Implement signature and graph validation**

Resolve annotations with `inspect.get_annotations(..., eval_str=True)` or the existing `get_type_hints(..., include_extras=True)` approach. Recognize a Spark dependency only when the resolved annotation is exactly `SparkSession`. Reject every other non-DataFrame parameter. Require a SparkSession parameter for sources and at least one DataFrame parameter for non-sources. Require an annotated DataFrame output for every step.

Use a DFS or Kahn topological sort to detect cycles. Match each upstream step's output schema to exactly one downstream DataFrame parameter. Reject duplicate matches, missing parameters, and upstream outputs that match no parameter. Store parameter-name-to-upstream-step bindings in an immutable execution plan.

- [ ] **Step 5: Run the pipeline validation tests**

Run: `uv run pytest tests/test_pipeline.py -q`

Expected: PASS.

### Task 3: Implement defensive execution

**Files:**
- Modify: `src/pyspark_schemas/pipeline.py`
- Modify: `tests/test_pipeline.py`

**Interfaces:**
- Consumes: Immutable execution plans from Task 2.
- Produces: `ExecutablePipeline.run(spark) -> dict[str, DataFrame]`.

- [ ] **Step 1: Write failing execution tests**

Test source execution with a SparkSession, single-input execution, multi-input fan-in, independent components, all-step output mapping, invalid SparkSession rejection, and transform failure wrapping:

```python
with pytest.raises(PipelineExecutionError, match="filtered") as error:
    executable.run(spark)

assert isinstance(error.value.__cause__, ValueError)
```

- [ ] **Step 2: Run the focused execution tests**

Run: `uv run pytest tests/test_pipeline.py -q`

Expected: FAIL because `ExecutablePipeline.run` is not implemented.

- [ ] **Step 3: Implement plan execution**

Validate that the runtime value is a `SparkSession`, then execute the precomputed topological order once. Build keyword arguments from the SparkSession binding and stored DataFrame bindings. Verify each result is a DataFrame and wrap transform exceptions in `PipelineExecutionError` using `raise ... from error`. Return outputs keyed by step name.

- [ ] **Step 4: Run the focused execution tests**

Run: `uv run pytest tests/test_pipeline.py -q`

Expected: PASS.

### Task 4: Export the public API and update documentation

**Files:**
- Modify: `src/pyspark_schemas/__init__.py`
- Modify: `docs/superpowers/specs/2026-09-09-pipeline-wiring-design.md`
- Modify: `docs/specs/dependencies.md`
- Test: `tests/test_pipeline.py`

**Interfaces:**
- Consumes: Completed pipeline implementation from Tasks 2 and 3.
- Produces: Package-level imports for `Pipeline`, `Step`, `ExecutablePipeline`, and `PipelineExecutionError`, plus examples that use `validate().run(spark)`.

- [ ] **Step 1: Add an import-level test**

Assert the public classes can be imported from `pyspark_schemas`.

- [ ] **Step 2: Update examples**

Change pipeline examples to use `connect`, `connect_many`, `validate`, and `ExecutablePipeline.run(spark)`. Remove or clearly mark the deferred `Depends` examples as future work so the documentation does not imply unsupported behavior.

- [ ] **Step 3: Export the classes**

Import the four public pipeline symbols from `pyspark_schemas.__init__` without changing existing module imports.

- [ ] **Step 4: Run the full validation suite**

Run: `just check`

Expected: PASS for Ruff, Pyrefly, formatting, and all tests.