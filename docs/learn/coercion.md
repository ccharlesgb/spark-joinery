# Coercing Schemas

Coercion in Spark Joinery is the process of automatically converting an input DataFrame to a specified schema. There
are several modes available to control how strict/relaxed the coercion process should be for your transformations.

Use `Schema(Model).coerce_dataframe()` to take an input DataFrame and return either a new DataFrame that
matches the model's schema or raise a validation error if coercion fails based on the mode's strictness.

By default Spark Joinery will project the input DataFrame
onto the desired schema, including nested struct fields. It will **not** perform any type casting, and it will
**not** add missing columns, this default mode is called `project`. The below example shows how this works
in practice, the input DataFrame has an extra column that is removed when we coerce it to the `Customer` model:

``` python
--8<-- "docs_src/learn/coercion/dataclass_coercion.py"
```

:fontawesome-solid-code: Outputs:

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

:fontawesome-solid-code: Outputs:

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

:fontawesome-solid-code: Outputs:

``` python
--8<-- "docs_src/learn/coercion/dataclass_coercion_strict_stdout.log"
```

### Project Top Level

`project_top_level` projects only the model's top-level columns. If there is a difference in struct fields it will fail:

``` python
--8<-- "docs_src/learn/coercion/dataclass_coercion_project_top_level.py"
```

:fontawesome-solid-code: Outputs:

``` python
--8<-- "docs_src/learn/coercion/dataclass_coercion_project_top_level_stdout.log"
```

### Project (Default)

The mode `project` projects nested struct fields, removing all fields that are not in
the model:

``` python
--8<-- "docs_src/learn/coercion/dataclass_coercion_project.py"
```

:fontawesome-solid-code: Outputs:

``` python
--8<-- "docs_src/learn/coercion/dataclass_coercion_project_stdout.log"
```

### Project Cast

The most relaxed mode, `project_cast`, recursively projects fields and casts values to the model's field types. This
will still fail if the types cannot be safely cast by spark but this can be useful if reading external data and
you want to easily align your DataFrame with the model's schema. It will also still fail if there are missing columns:

``` python
--8<-- "docs_src/learn/coercion/dataclass_coercion_project_cast.py"
```

:fontawesome-solid-code: Outputs:

``` python
--8<-- "docs_src/learn/coercion/dataclass_coercion_project_cast_stdout.log"
```

Type casting in Spark Joinery follows the Spark casting rules described in
the [spark documentation](https://spark.apache.org/docs/latest/sql-ref-ansi-compliance.html#cast). If the cast is
permitted then the field will be wrapped in a `cast` function to try to change the data type. This could still
raise a runtime error if the cast is not possible for a specific value in your dataframe.
