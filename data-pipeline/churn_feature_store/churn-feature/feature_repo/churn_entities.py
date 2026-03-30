from feast import Entity
from feast.value_type import ValueType

customer = Entity(
    name="customer",
    description="A customer entity with unique ID",
    join_keys=["customer_id"],
    value_type=ValueType.STRING,
    tags={"owner": "data_team", "domain": "customer_analytics", "team": "mlops"},
)
