# Coercing Schemas

Now that we can define schemas we can use the coercion functionality to automatically convert an input DataFrame to 
match a specified model. 

```python
--8<-- "docs_src/learn/coercion/dataclass_coercion.py"
```

Which outputs:

```python
--8<-- "docs_src/learn/coercion/dataclass_coercion_stdout.log"
```

## Coercion Modes

There are four modes for coercion:

### Strict

In strict mode, the input DataFrame must exactly match the specified model. Any extra columns, missing columns or type mismatches will result in an error:

```python
--8<-- "docs_src/learn/coercion/dataclass_coercion_strict.py"
```

Which outputs:

```python
--8<-- "docs_src/learn/coercion/dataclass_coercion_strict_stdout.log"
```

### Strict Null

`strict_null` also compares column nullability. This DataFrame has the right fields
and types, but its non-nullable columns do not match the model's nullable schema:

```python
--8<-- "docs_src/learn/coercion/dataclass_coercion_strict_null.py"
```

Which outputs:

```python
--8<-- "docs_src/learn/coercion/dataclass_coercion_strict_null_stdout.log"
```

### Project

`project` keeps only the model's top-level columns. The selected columns must
already have matching types.

```python
--8<-- "docs_src/learn/coercion/dataclass_coercion_project.py"
```

Which outputs:

```python
--8<-- "docs_src/learn/coercion/dataclass_coercion_project_stdout.log"
```

### Project All

`project_all` also projects nested struct fields, removing fields that are not in
the model.

```python
--8<-- "docs_src/learn/coercion/dataclass_coercion_project_all.py"
```

Which outputs:

```python
--8<-- "docs_src/learn/coercion/dataclass_coercion_project_all_stdout.log"
```

### Coerce

`coerce` recursively projects fields and casts values to the model's field types.

```python
--8<-- "docs_src/learn/coercion/dataclass_coercion_coerce.py"
```

Which outputs:

```python
--8<-- "docs_src/learn/coercion/dataclass_coercion_coerce_stdout.log"
```
