from spark_joinery import get_spark_schema_from_model
from dataclasses import dataclass
from typing import Annotated
from decimal import Decimal
from pyspark.sql.types import DecimalType


@dataclass
class Customer:
    customer_id: str
    name: str
    is_active: bool
    discount_rate: float
    max_billing_amount: Annotated[Decimal, DecimalType(precision=10, scale=2)]


schema = get_spark_schema_from_model(Customer)
print(schema)
