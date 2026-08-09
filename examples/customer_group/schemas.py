from dataclasses import dataclass


@dataclass
class Customer:
    customer_id: str
    name: str
    parent_customer_id: str | None = None


@dataclass
class CustomerGroup:
    customer_id: str
    name: str
    parent_customer_id: str | None = None
    parent_name: str | None = None
