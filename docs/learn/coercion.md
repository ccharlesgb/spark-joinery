# Coercing Schemas

Coercion in Spark Joinery is the process of automatically converting an input DataFrame to a specified schema. There
are several modes available to control how strict/relaxed the coercion process should be for your transformations.

We can use the method `coerce_dataframe_to_model` to take an input DataFrame and return either a new DataFrame that
matches a models schema or raise a validation error if the coercion fails based no the mode's strictness.

By default Spark Joinery will project the input DataFrame
onto the desired schema, including nested struct fields. It will **not** perform any type casting, and it will
**not** add missing columns, this default mode is called `project_all`. The below example shows how this works
in practice, the input DataFrame has an extra column that is removed when we coerce it to the `Customer` model:

``` python
--8<-- "docs_src/learn/coercion/dataclass_coercion.py"
```

Which outputs:

``` python
--8<-- "docs_src/learn/coercion/dataclass_coercion_stdout.log"
```

## Coercion Modes

Depending on your workflow and needs, you can choose from several coercion modes.

### Strict Null

The most strict `strict_null` enforces an exact schema match with column nullability. This DataFrame has the right fields
and types, but its non-nullable columns do not match the model's nullable schema:

``` python
--8<-- "docs_src/learn/coercion/dataclass_coercion_strict_null.py"
```

Which outputs:

``` python
--8<-- "docs_src/learn/coercion/dataclass_coercion_strict_null_stdout.log"
```

### Strict

In strict mode, the input DataFrame must exactly match the specified model. Any extra columns,
missing columns or type mismatches will result in an error, it will however ignore nullability. This DataFrame
has an extra column so will fail the `strict` validation check:

``` python
--8<-- "docs_src/learn/coercion/dataclass_coercion_strict.py"
```

Which outputs:

``` python
--8<-- "docs_src/learn/coercion/dataclass_coercion_strict_stdout.log"
```

### Project

`project` projects only the model's top-level columns. If there is a difference in struct fields it will fail:

``` python
--8<-- "docs_src/learn/coercion/dataclass_coercion_project.py"
```

Which outputs:

``` python
--8<-- "docs_src/learn/coercion/dataclass_coercion_project_stdout.log"
```

### Project All (Default)

As explained before `project_all` also projects nested struct fields, removing all fields that are not in
the model:

``` python
--8<-- "docs_src/learn/coercion/dataclass_coercion_project_all.py"
```

Which outputs:

``` python
--8<-- "docs_src/learn/coercion/dataclass_coercion_project_all_stdout.log"
```

### Coerce

The most relaxed mode, `coerce`, recursively projects fields and casts values to the model's field types. This
will still fail if the types cannot be safely cast by spark but this can be useful if reading external data and
you want to easily align your DataFrame with the model's schema. It will also still fail if there are missing columns:

``` python
--8<-- "docs_src/learn/coercion/dataclass_coercion_coerce.py"
```

Which outputs:

``` python
--8<-- "docs_src/learn/coercion/dataclass_coercion_coerce_stdout.log"
```

Type casting in Spark Joinery follows the Spark casting rules described in
the [spark documentation](https://spark.apache.org/docs/latest/sql-ref-ansi-compliance.html#cast). If the cast is
permitted then the field will be wrapped in a `cast` function to try to change the data type. This could still
raise a runtime error if the cast is not possible for a specific value in your dataframe.
