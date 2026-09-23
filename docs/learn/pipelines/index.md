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

## Passing model objects

Pipeline steps can also pass typed non-DataFrame values. This example reads housing data,
prepares a feature vector from `number_of_bedrooms` and `square_footage`, fits a linear
regression model for `price`, and prints its coefficients:

``` python
--8<-- "docs_src/learn/pipelines/index/pipeline_connect_non_dataframe_output.py"
```

:fontawesome-solid-code: Outputs:

``` md
--8<-- "docs_src/learn/pipelines/index/pipeline_connect_non_dataframe_output_stdout.log"
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

The pipeline will also fail to run if you have left a transformation 'dangling', meaning that it
is missing a connection for one of it's upstream dependencies:

``` python
--8<-- "docs_src/learn/pipelines/index/pipeline_connect_dangling_transformation.py"
```

:fontawesome-solid-code: Outputs:

``` md
--8<-- "docs_src/learn/pipelines/index/pipeline_connect_dangling_transformation_stdout.log"
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

## Non dataframe inputs/outputs

Pipeline steps can accept and produce non-DataFrame values. This allows you to pass around
typed objects such as machine learning models, pandas DataFrames or any general python object
between steps in the pipeline. The pipeline will only validate that the type matches between the
input and output and won't do any schema coercion for pandas DataFrames. The below example shows
a common use case where data is read, then features are engineered and an ML model is trained. You
could then write this to MLflow for experiment tracking and model management:

``` python
--8<-- "docs_src/learn/pipelines/index/pipeline_connect_non_dataframe_output.py"
```

:fontawesome-solid-code: Outputs:

``` md
--8<-- "docs_src/learn/pipelines/index/pipeline_connect_non_dataframe_output_stdout.log"
```
