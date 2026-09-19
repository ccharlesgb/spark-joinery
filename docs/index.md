# Spark Joinery

<figure markdown="span">
![Image title](/images/spark-joinery-logo.png){ width="300" }
<figcaption>Schema first spark transformations</figcaption>
</figure>

## Overview

Spark Joinery is a small framework that allows you to build complex spark jobs with an emphasis on
testability. It helps you break apart complex transformations into testable units, whilst
allowing you to annotate these transformations with the upstream/downstream schemas. In pyspark
codebases you often see a transformation declared as:

``` python
def get_metrics(fact_table: DataFrame) -> DataFrame:
    ...
```

From this we have no idea what the inputs or outputs of the transformation are. With Spark Joinery you
can make it much clearer:

``` python
--8<-- "docs_src/index/intro.py:5:"
```

Using a schema first approach makes it much clearer what this transformation does, for
both you and a coding agent. Spark Joinery will also enforce at runtime that the input and output
schemas match the type annotations.

## Installation

Install the package `spark-joinery` with your favourite package manager:

``` bash title="Install with pip"
pip install spark-joinery
```

``` bash title="Install with uv"
uv install spark-joinery
```

## Example

A minimum example pipeline is:

``` python
--8<-- "docs_src/index/minimum_example.py"
```

Which outputs:

``` md
--8<-- "docs_src/index/minimum_example_stdout.log"
```
