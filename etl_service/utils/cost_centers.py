import pandas as pd
import numpy as np
from typing import List, Tuple, Any, Set, Dict, Union


def cost_centers(
    data_frame: pd.DataFrame,
    catalog: pd.DataFrame,
    code: str,
    id: str,
    cost_center: str,
    percentage_cost_center: str
) -> Tuple[List[List[Any]], List[List[Any]]]:
    """
    Only usefull for prorrateo employees

    Processes employee cost center allocations based on percentages.
    
    This function takes employee IDs from a data frame and looks up their 
    cost center allocations in a catalog, creating two lists:
    1. Prorated cost centers with their percentages
    2. Default allocations for employees without valid percentage data
    
    Args:
        data_frame: DataFrame containing employee IDs
        catalog: DataFrame containing cost center allocation data
        code: Column name in data_frame for employee IDs
        id: Column name in catalog for employee IDs
        cost_center: Column name in catalog for cost center values
        percentage_cost_center: Column name in catalog for percentage allocations
    
    Returns:
        A tuple containing:
        - cost_center_prorrateo: List of [employee_id, percentage, cost_center] for employees with valid percentage data
        - cost_center_list: List of [employee_id, 1.0, None] for employees without valid percentage data
    """
    cost_center_prorrateo = []
    cost_center_list = []
    
    # Convert catalog ID column to string for reliable matching
    catalog = catalog.copy()
    catalog[id] = catalog[id].astype(str)
    
    # Create a set of valid employee IDs for faster lookup
    valid_employee_ids = set(catalog[id].values)
    
    # Process each employee ID
    for employee_id in data_frame[code].astype(str):
        if employee_id in valid_employee_ids:
            # Get filtered rows for this employee
            employee_data = catalog[catalog[id] == employee_id]
            
            # Extract percentages and cost centers
            percentages = employee_data[percentage_cost_center].values
            cc_values = employee_data[cost_center].values
            
            # Flag to track if we found any valid entries
            has_valid_entries = False
            
            # Process each percentage and cost center pair
            for percentage, cc_value in zip(percentages, cc_values):
                # Check if percentage is valid (greater than 0)
                if pd.notna(percentage) and float(percentage) > 0:
                    cost_center_prorrateo.append([
                        employee_id, 
                        float(percentage) / 100, 
                        cc_value
                    ])
                    has_valid_entries = True
            
            # If no valid entries were found for this employee, add to default list
            if not has_valid_entries:
                cost_center_list.append([employee_id, 1.0, None])
    
    return cost_center_prorrateo, cost_center_list