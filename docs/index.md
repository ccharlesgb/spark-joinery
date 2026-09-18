# Spark Joinery

Schema first spark transformations

## Overview

Spark Joinery is a small framework that allows you to build complex spark jobs with an emphasis on 
testability. It emphasises breaking apart complex transformations into testable units, whilst 
requiring you to annotate these transformations with the upstream/downstream schemas. In pyspark 
code bases you often see a transformation declared as:

```python
def get_metrics(fact_table: DataFrame) -> DataFrame:
    ...
```

From this we have no idea what the inputs or outputs of the transformation are. With Spark Joinery you 
can make it much clearer:

```python
--8<-- "docs_src/index.py:5:"
```

Using a schema first approach makes it much clearer what this transformation should. For both you and a coding agent.

## Installation

Install the package `spark-joinery` with your favourite package manager:

```bash title="Install with pip"
pip install spark-joinery
```

```bash title="Install with uv"
uv install spark-joinery
```


