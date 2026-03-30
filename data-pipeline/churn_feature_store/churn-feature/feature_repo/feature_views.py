from feast import FeatureView, Field
from feast.types import Float32, Int64, Bool
from datetime import timedelta
from churn_entities import customer
from data_sources import customer_stats_source

# Customer demographics feature view
customer_demographics = FeatureView(
    name="customer_demographics",
    entities=[customer],
    ttl=None,
    schema=[
        Field(name="age", dtype=Float32),
        Field(name="gender_male", dtype=Bool),
        Field(name="tenure_months", dtype=Float32),
        Field(name="tenure_age_ratio", dtype=Float32),
        Field(name="subscription_type_premium", dtype=Bool),
        Field(name="subscription_type_standard", dtype=Bool),
        Field(name="contract_length_monthly", dtype=Bool),
        Field(name="contract_length_quarterly", dtype=Bool),
        Field(name="tenure_group_1_to_2yr", dtype=Bool),
        Field(name="tenure_group_2_to_3yr", dtype=Bool),
        Field(name="tenure_group_3plus_yr", dtype=Bool),
    ],
    source=customer_stats_source,
    online=True,
)

customer_behavior = FeatureView(
    name="customer_behavior",
    entities=[customer],
    ttl=None,
    schema=[
        Field(name="usage_frequency", dtype=Float32),
        Field(name="support_calls", dtype=Float32),
        Field(name="payment_delay_days", dtype=Float32),
        Field(name="total_spend", dtype=Float32),
        Field(name="last_interaction_days", dtype=Float32),
        Field(name="spend_per_usage", dtype=Float32),
        Field(name="support_calls_per_tenure", dtype=Float32),
        Field(name="avg_monthly_spend", dtype=Float32),
        Field(name="churn_risk_score", dtype=Float32),
        Field(name="spending_group_medium", dtype=Bool),
        Field(name="spending_group_high", dtype=Bool),
        Field(name="spending_group_very_high", dtype=Bool),
    ],
    source=customer_stats_source,
    online=True,
)

# Churn target feature view
churn_target = FeatureView(
    name="churn_target",
    entities=[customer],
    ttl=None,
    schema=[
        Field(name="churned", dtype=Int64),
    ],
    source=customer_stats_source,
    online=True,
)
