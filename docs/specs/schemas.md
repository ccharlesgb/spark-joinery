# Schema Definition

## Dataclasses

Dataclass can be used to define a spark schema like so:

```python
    @dataclass
    class NestedDataClass:
        nested_field: int

    @dataclass
    class MyDataClass:
        array_field: list[str]
        optional_array_field: Optional[list[int]]
        array_field_with_optional_elements: list[Optional[float]]
        array_field_of_datclasses: list[NestedDataClass]
        array_field_of_optional_datclasses: list[Optional[NestedDataClass]]
        optional_array_field_of_optional_datclasses: Optional[
            list[Optional[NestedDataClass]]
        ]
```

# Pydantic

If you want extra runtime validation in your unit tests you can use pydantic models as well to add more complex validation rules:

```python
    class Product(BaseModel):
        product_id: int
        product_name: str
```

## Modifying Schemas

A common pattern that comes up is prefixing column names (For example after a join) the syntax to do this is:

```python
    @dataclass
    class User:
        id: int
        name: str
        country: str
        department_id: int

    UsersAfterJoin = prefix_column_names(Users, "user_")

    # Equivalent of doing
    @dataclass
    class UsersAfterJoin:
        user_id: int
        user_name: str
        user_country: str
```

You can also combine schemas if there are no name collisions:

```python
    @dataclass
    class User:
        id: int
        name: str
        country: str
        department_id: int

    @dataclass
    class Department:
        id: int
        name: str

    UserWithDepartment = combine_schemas(prefix_column_names(User, "user_"), prefix_column_names(Department, "department_"))

    # Equivalent of doing
    @dataclass
    class UserWithDeparment:
        user_id: int
        user_name: str
        user_country: str
        user_department_id: int
        department_id: int
        deparment_name: str
```

Note: Under the hood this uses `make_dataclass` so will not be checked by type checkers

