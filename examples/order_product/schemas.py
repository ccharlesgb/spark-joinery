from datetime import datetime


from dataclasses import dataclass


@dataclass
class Order:
    order_id: int
    customer_id: str
    order_timestamp: datetime
    product_id: int
    quantity: int


@dataclass
class Customer:
    customer_id: str
    name: str


@dataclass
class OrderWithCustomerDimension(Order, Customer):
    pass
