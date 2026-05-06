"""Cleaning and aggregation."""
import pandas as pd


def clean_data(df: pd.DataFrame, missing_fill: float = 0) -> pd.DataFrame:
    """Remove duplicates and handle missing amounts.
    
    Args:
        df: Input DataFrame
        missing_fill: Value to fill missing amounts (default: 0)
        
    Returns:
        Cleaned DataFrame
    """
    df = df.drop_duplicates().copy()
    df["amount"] = df["amount"].fillna(missing_fill)
    return df


def aggregate_by_account(df: pd.DataFrame) -> pd.DataFrame:
    """Total transaction amount per account."""
    return df.groupby("account_id")["amount"].sum().reset_index()
