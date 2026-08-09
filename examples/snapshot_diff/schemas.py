from datetime import date, datetime


from dataclasses import dataclass


@dataclass
class SnapshottedDimension:
    snapshot_date: date
    entity_id: str
    dimension_field1: str
    dimension_field2: str
    dimension_updated_at: datetime


@dataclass
class DimensionTransitions:
    snapshot_date: date
    entity_id: str
    old_dimension_field1: str
    old_dimension_field2: str
    new_dimension_field1: str
    new_dimension_field2: str
    dimension_updated_at: datetime
