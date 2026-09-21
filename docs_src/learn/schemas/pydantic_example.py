from spark_joinery import Schema
from pydantic import BaseModel, Field
from typing import Annotated
from decimal import Decimal
from pyspark.sql.types import DecimalType


class Customer(BaseModel):
    customer_id: str
    name: str
    is_active: bool
    discount_rate: float = Field(default=0.0, ge=0.0, le=1.0)
    max_billing_amount: Annotated[Decimal, DecimalType(precision=10, scale=2)]


schema = Schema(Customer)
print(schema.pretty_schema)
