from spark_joinery import Schema
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


schema = Schema(Customer)
print(schema.pretty_schema)
