# Spark Joinery

---

**Documentation**: [https://ccharlesgb.github.io/spark-joinery/](https://ccharlesgb.github.io/spark-joinery/)

**Examples**: [https://github.com/ccharlesgb/spark-joinery-examples](https://github.com/ccharlesgb/spark-joinery-examples)

---

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
from dataclasses import dataclass
from datetime import date
from pyspark.sql import DataFrame
from typing import Annotated
from spark_joinery import transform, Strict


@dataclass
class Customer:
    snapshot_date: date
    customer_id: str
    name: str
    is_active: bool
    package: str


@dataclass
class CustomerMetrics:
    snapshot_date: date
    total_customer_count: int
    active_customer_count: int
    basic_package_customer_count: int
    pro_package_customer_count: int


@transform
def get_metrics(
    fact_table: Annotated[DataFrame, Strict(Customer)],
) -> Annotated[DataFrame, Strict(CustomerMetrics)]: ...
```

Using a schema first approach makes it much clearer what this transformation does, for
both you and a coding agent. Spark Joinery will also enforce at runtime that the input and output
schemas match the type annotations.

## Development

This project uses `uv` for Python/dependency management and `just` as the command runner.

### Quick start

```bash
just install
```

### Common commands

```bash
just test
just lint
just format
just check
```
