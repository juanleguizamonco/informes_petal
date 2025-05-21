import pandas as pd

def safe_get_value(df: pd.DataFrame, column: str, default: str) -> str:
    """Helper function to safely get a value from a DataFrame column with a default."""
    if column in df.columns and not df[column].empty and not pd.isna(df[column].iloc[0]):
        return str(df[column].iloc[0])
    return default