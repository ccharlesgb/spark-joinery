from spark_joinery import get_spark_schema_from_model
from dataclasses import dataclass


@dataclass
class Location:
    latitude: float
    longitude: float


@dataclass
class Employee:
    name: str
    phone_number: str


@dataclass
class Customer:
    customer_id: str
    name: str
    employees: list[Employee]
    location: Location


schema = get_spark_schema_from_model(Customer)
print(schema)
