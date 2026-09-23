# Pipelines

!!! example

    Pipeline syntax is experimental

Pipelines allow you to chain together multiple transformations. The pipeline will validate
that the output of each transformation is compatible with the input of the next one.

## Defining a pipeline

To define a pipeline first create all your transformations and input/output schemas. Then create
your pipeline class and add each transformation as a `Step`. A `Step` represents a single
instance of that transformation within the pipeline. This distinction allows you to reuse the same
transformation multiple times within the same pipeline. Once the transformations have been added
you must connect them together using the `connect` method. This method will verify that the
transformations are compatible and can be connected together:

``` python
--8<-- "docs_src/learn/pipelines/index/pipeline_connect_happy.py"
```

:fontawesome-solid-code: Outputs:

``` md
--8<-- "docs_src/learn/pipelines/index/pipeline_connect_happy_stdout.log"
```

## Validating the pipeline

The pipeline will validate that itself as you connect transformations. If there is a schema
incompatibility then you will see a `PipelineConnectionError`. An example of this is shown below:

``` python
--8<-- "docs_src/learn/pipelines/index/pipeline_connect_mismatch.py"
```

:fontawesome-solid-code: Outputs:

``` md
--8<-- "docs_src/learn/pipelines/index/pipeline_connect_mismatch_stdout.log"
```

## Detecting cycles

The pipeline will automatically detect cycles as you build it to ensure that the end result is
runnable. The below example shows how the error is raised as you are connecting transformations:

``` python
--8<-- "docs_src/learn/pipelines/index/pipeline_connect_cycle.py"
```

:fontawesome-solid-code: Outputs:

``` md
--8<-- "docs_src/learn/pipelines/index/pipeline_connect_cycle_stdout.log"
```
