from etl_service.utils.cost_centers import cost_centers
from etl_service.utils.amount_calc import amount_calculation
from etl_service.utils.row_creation import gl_row_creation
from etl_service.utils.level_specifications import get_level_specifications
from etl_service.utils.get_value import safe_get_value

import pandas as pd
from typing import Dict, List, Union, Any, Set, Optional

def employee_process(
        employee_data: pd.DataFrame, 
        df_catalog1: pd.DataFrame, 
        df_catalog2: pd.DataFrame, 
        df_catalog3: pd.DataFrame, 
        df_catalog5: pd.DataFrame,
        period: str, 
        entity: str, 
        pay_group: str, 
        work_agreement:str, 
        employee_name: str, 
        employee_id: str
) -> pd.DataFrame:
    """
    Processes employee payroll data for accounting entries.
    
    This function creates accounting entries for an employee based on their payroll data
    and various catalog configurations. It handles prorated cost centers and calculates
    appropriate amounts for each paycode.
    
    Args:
        employee_data: DataFrame containing employee's payroll data
        df_catalog1: Catalog containing paycode configuration
        df_catalog2: Catalog containing cost center configuration
        df_catalog3: Catalog containing level specifications
        df_catalog5: Additional catalog (currently unused)
        period: Column name for payroll period
        entity: Column name for legal entity
        pay_group: Column name for pay group
        work_agreement: Column name for work agreement
        employee_name: Column name for employee name
        employee_id: Column name for employee ID
    
    Returns:
        DataFrame containing detailed accounting entries for the employee
    """
    detailed_columns = [
        "Payroll Period", "Entity", "Pay group", "Work Agreement", "EE ID", "Employee Name",
        "Client ID", "Client Description", "% CC", "Level 1","Level2 (Optional)",
        "Level3 (Optional)", "Level4 (Optional)", "Amount", "Debt Account", "Acred Account",
        "Item Text", "GL File", "Observations"
    ]
    try:
        period_e = str(employee_data[period].iloc[0])
        entity_e = employee_data[entity].iloc[0]

        # Use safe getters with fallbacks for optional fields
        pay_group_e = safe_get_value(employee_data, pay_group, 'Paygroup not found')
        work_agreement_e = safe_get_value(employee_data, work_agreement, 'Work agreement not found')
        employee_name_e = safe_get_value(employee_data, employee_name, "")
        
        # Convert employee_id to int safely
        employee_id_e = int(employee_data[employee_id].values[0])
    except (IndexError, ValueError, KeyError) as e:
        # Return empty DataFrame if core employee data is missing
        print(f"Error processing employee data: {e}")
        return pd.DataFrame(columns=detailed_columns)
    
    #Get level specifications from catalog3
    levels = get_level_specifications(df_catalog3)

    #Define columns to skip during processing
    base_columns = [
        'PERIOD/Payroll Type', 'Legal', 'EMPLOYEE_ID', 'CO30720.000-IFGDNACNN',
        'CO30730.000-IFGDNACNN', 'ME01010.000-ISGDDMFNN', 'ME01410.000-ISGDDMFNN',
        'ME02010.000-SSGDDMFNN', 'ME41210.000-ISGDDMFNN'
        ]
    
    #Track processed combinations to avoid duplicates
    processed_keys: Set[tuple] = set()

    #List to store analysis results
    analysis = []

    #Process each paycode
    for paycode in employee_data.columns:
        #skip base columns
        if paycode in base_columns:
            continue

        #Get paycode value
        value = employee_data.iloc[0][paycode]

        #Skip if value is NA
        if pd.isna(value):
            continue

        # Calculate amounts
        amounts = amount_calculation(
            value, 100, df_catalog1, paycode, employee_id, 
            df_catalog2, 'Petal Code', 'Accounting type', 'DEBIT account',
            'CREDIT account', 'Client Code', 'Campo', 'GLFile', 'Item TEXT', 
            'Employee ID', 'Tipo', 'BENEFICIARIA', 'NUM. ACREEDOR'
        )
            
        # Create rows for each amount
        for _, amount_row in amounts.iterrows():
            new_row = gl_row_creation(
                period_e, entity_e, pay_group_e, work_agreement_e, employee_id_e, employee_name_e,
                amount_row['Client Code'], amount_row['Campo'], 100, levels['level1'],levels['level2'],
                levels['level3'], levels['level4'], amount_row['Amount'], amount_row['DEBIT account'], 
                amount_row['CREDIT account'],amount_row.get('Item Text', ''), amount_row['GLFile']
            )
            analysis.append(new_row)
    
    # Create final DataFrame, filter out zero amounts, and return
    if not analysis:
        return pd.DataFrame(columns=detailed_columns)
    
    df_analysis = pd.DataFrame(analysis)
    return df_analysis[df_analysis['Amount'] != 0]
