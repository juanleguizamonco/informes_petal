
import pandas as pd
from typing import Tuple, List, Union, Optional


def catalogs_filter(
    df_catalog: pd.DataFrame,
    catalog1_start: int,
    catalog1_end: int,
    catalog2_start: int,
    catalog2_end: int,
    catalog3_start: int,
    catalog3_end: int,
    catalog4_start: int,
    catalog4_end: int,
    catalog5_start: int,
    catalog5_end: int
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Filters and processes different catalogs from a main DataFrame.
    
    This function takes a source DataFrame and extracts three separate catalogs
    based on the specified column ranges. It also handles header formatting
    and data cleaning.
    
    Args:
        df_catalog: Source DataFrame containing all catalog data
        catalog1_start: Starting column index for catalog 1
        catalog1_end: Ending column index for catalog 1 (exclusive)
        catalog2_start: Starting column index for catalog 2
        catalog2_end: Ending column index for catalog 2 (exclusive)
        catalog3_start: Starting column index for catalog 3
        catalog3_end: Ending column index for catalog 3 (exclusive)
    
    Returns:
        A tuple containing three DataFrames:
        - df_catalog1: First catalog DataFrame
        - df_catalog2: Second catalog DataFrame with NaN values removed
        - df_catalog3: Third catalog DataFrame
    """
    # Create a copy to avoid modifying the original DataFrame
    df_catalogs = df_catalog.copy()
    
    # Extract only data rows (skip first row)
    if len(df_catalogs) > 1:
        df_catalogs = df_catalogs.iloc[1:, :]
    
    # Use second row as column headers
    if len(df_catalogs) > 0:
        headers = df_catalogs.iloc[0, :].values
        df_catalogs.columns = headers
        
        # Remove the header row that's now used as column names
        df_catalogs = df_catalogs.iloc[1:].reset_index(drop=True)
        
        # Process catalog 1: Extract specified columns
        df_catalog1 = df_catalogs.iloc[:, catalog1_start:catalog1_end].copy()
        
        # Process catalog 2: Extract specified columns and drop rows with all NaN values
        df_catalog2 = df_catalogs.iloc[:, catalog2_start:catalog2_end].copy().dropna(how='all')
        
        # Process catalog 3: Extract specified columns
        df_catalog3 = df_catalogs.iloc[:, catalog3_start:catalog3_end].copy()

        # Process catalog 3: Extract specified columns
        df_catalog4 = df_catalogs.iloc[:, catalog4_start:catalog4_end].copy()

        # Process catalog 3: Extract specified columns
        df_catalog5 = df_catalogs.iloc[:, catalog5_start:catalog5_end].copy()
        
        return df_catalog1, df_catalog2, df_catalog3, df_catalog4, df_catalog5
    
    # Return empty DataFrames if input is empty or has insufficient rows
    return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame()