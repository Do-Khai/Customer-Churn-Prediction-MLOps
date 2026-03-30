import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import pyarrow as pa
import pyarrow.parquet as pq
import os


def prepare_data_for_feast(input_path, output_path="data/processed_churn_data.parquet"):
    """
    Convert processed churn data to Feast-compatible format
    """
    # Load your processed data
    df = pd.read_csv(input_path)

    # Add timestamp columns required by Feast
    current_time = datetime.now()

    # Create event timestamp (simulate data from last 90 days)
    df["event_timestamp"] = current_time - pd.to_timedelta(
        np.random.randint(0, 90 * 24 * 60 * 60, size=len(df)), unit="s"
    )

    # Create created timestamp (when feature was computed)
    df["created_timestamp"] = current_time

    # Ensure customer_id is string (without .0 from floats)
    df["customer_id"] = df["CustomerID"].astype(int).astype(str)

    # Map column names to Feast-compatible names
    column_mapping = {
        "Age": "age",
        "Tenure": "tenure_months",
        "Usage Frequency": "usage_frequency",
        "Support Calls": "support_calls",
        "Payment Delay": "payment_delay_days",
        "Total Spend": "total_spend",
        "Last Interaction": "last_interaction_days",
        "Churn": "churned",
        "Tenure_Age_Ratio": "tenure_age_ratio",
        "Spend_per_Usage": "spend_per_usage",
        "Support_Calls_per_Tenure": "support_calls_per_tenure",
        "Gender_Male": "gender_male",
        "Subscription Type_Premium": "subscription_type_premium",
        "Subscription Type_Standard": "subscription_type_standard",
        "Contract Length_Monthly": "contract_length_monthly",
        "Contract Length_Quarterly": "contract_length_quarterly",
        "Spending_Group_Medium": "spending_group_medium",
        "Spending_Group_High": "spending_group_high",
        "Spending_Group_Very High": "spending_group_very_high",
        "Tenure_Group_1-2yr": "tenure_group_1_to_2yr",
        "Tenure_Group_2-3yr": "tenure_group_2_to_3yr",
        "Tenure_Group_3+yr": "tenure_group_3plus_yr",
    }

    # Rename columns
    df = df.rename(columns=column_mapping)

    # Add any missing engineered features
    if "avg_monthly_spend" not in df.columns:
        df["avg_monthly_spend"] = df["total_spend"] / np.maximum(df["tenure_months"], 1)

    if "churn_risk_score" not in df.columns:
        # Simple risk score calculation
        df["churn_risk_score"] = (
            df["payment_delay_days"] * 0.3
            + (df["support_calls"] / np.maximum(df["tenure_months"], 1)) * 0.2
            + (1 - (df["last_interaction_days"] / 30)) * 0.5
        ).clip(0, 1)

    # Select and order columns for Feast
    feast_columns = [
        "customer_id",
        "event_timestamp",
        "created_timestamp",
        "age",
        "gender_male",
        "tenure_months",
        "usage_frequency",
        "support_calls",
        "payment_delay_days",
        "subscription_type_premium",
        "subscription_type_standard",
        "contract_length_monthly",
        "contract_length_quarterly",
        "spending_group_medium",
        "spending_group_high",
        "spending_group_very_high",
        "tenure_group_1_to_2yr",
        "tenure_group_2_to_3yr",
        "tenure_group_3plus_yr",
        "total_spend",
        "last_interaction_days",
        "tenure_age_ratio",
        "spend_per_usage",
        "support_calls_per_tenure",
        "avg_monthly_spend",
        "churn_risk_score",
        "churned",
    ]

    df_feast = df[feast_columns].copy()
    
    # Ensure churn target is safely integer matched to the schema
    df_feast["churned"] = df_feast["churned"].astype(int)

    # Save as Parquet (Feast recommended format)
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df_feast.to_parquet(output_path, index=False)

    print(f"Data prepared for Feast. Shape: {df_feast.shape}")
    print(f"Saved to: {output_path}")

    return df_feast


if __name__ == "__main__":
    # Get the directory
    script_dir = os.path.dirname(os.path.abspath(__file__))

    # Navigate exactly to the processed data relative to this script
    input_file = os.path.join(
        script_dir, "../../../data/processed/df_processed_period_1.csv"
    )
    output_file = os.path.join(script_dir, "data/processed_churn_data.parquet")

    prepare_data_for_feast(input_file, output_path=output_file)
