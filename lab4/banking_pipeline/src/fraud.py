"""Fraud / high-value flags."""
import pandas as pd


def detect_fraud(df: pd.DataFrame, threshold: float = 800) -> pd.DataFrame:
    """Flag high-value transactions.
    
    Args:
        df: Input DataFrame with amount column
        threshold: Amount above which transaction is flagged as fraud
        
    Returns:
        DataFrame with added is_fraud column
    """
    out = df.copy()
    out["is_fraud"] = out["amount"] > threshold
    return out
