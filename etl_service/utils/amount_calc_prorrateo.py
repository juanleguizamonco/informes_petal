import pandas as pd
from typing import Dict, List, Optional, Union, Any


def amount_calculation_prorrateo(
    value: float,
    percentage_cc: float,
    catalog: pd.DataFrame,
    paycode: Union[str, int],
    employee_id: str,
    catalog2: pd.DataFrame,
    petal_code: str,
    account_type: str,
    deb_account: str,
    cred_account: str,
    cli_code: str,
    camp: str,
    file_gl: str,
    item: str,
    employee_id_code: str,
    type_cat: str,
    recipient: str,
    cred_account_recipient: str
) -> pd.DataFrame:
    """
    Calculates prorated amounts for financial transactions based on payment codes.
    
    Args:
        value: Base value for calculation
        percentage_cc: Percentage to apply for cost center proration
        catalog: Main catalog DataFrame
        paycode: Payment code to look up
        employee_id: Employee ID
        catalog2: Secondary catalog DataFrame
        petal_code: Column name for petal code in catalog
        account_type: Column name for account type
        deb_account: Column name for debit account
        cred_account: Column name for credit account
        cli_code: Column name for client code
        camp: Column name for field
        file_gl: Column name for GL file
        item: Column name for item text
        employee_id_code: Column name for employee ID in catalog2
        type_cat: Column name for type in catalog2
        recipient: Column name for recipient in catalog2
        cred_account_recipient: Column name for recipient's credit account
    
    Returns:
        DataFrame with calculated prorated amounts and corresponding accounting information
    """
    # Convert paycode to string and remove whitespace
    paycode_str = str(paycode).strip()
    
    # Prepare petal_code column for comparison
    catalog[petal_code] = catalog[petal_code].astype(str).str.strip()
    
    # Filter catalog rows that match the paycode
    catalog_matches = catalog[catalog[petal_code] == paycode_str]
    
    # If no matches, return empty DataFrame
    if catalog_matches.empty:
        return pd.DataFrame()
    
    # Calculate prorated base amount
    base_amount = float(value) * percentage_cc
    
    # List to store results
    amounts = []
    
    # Pre-filter catalog2 for the specific employee_id (avoids repeating in each iteration)
    df_employee = catalog2[catalog2[employee_id_code] == employee_id] if 'P+' in catalog_matches[account_type].values or 'N+' in catalog_matches[account_type].values else None
    
    # Process each matching catalog row
    for _, row in catalog_matches.iterrows():
        accounting_type = row.get(account_type)
        debit_account = row.get(deb_account)
        credit_account = row.get(cred_account)
        client_code = row.get(cli_code)
        campo = row.get(camp)
        gl_file = row.get(file_gl)
        item_text = row.get(item)
        
        # Check if there are valid accounts to process
        if pd.notna(debit_account) or pd.notna(credit_account):
            # Determine amount sign based on accounting type
            if accounting_type in ['D-', 'C+']:
                amount = -base_amount
            elif accounting_type in ['P+', 'N+'] and df_employee is not None and not df_employee.empty:
                amount = base_amount
                # Process special entries from catalog2
                for _, emp_row in df_employee.iterrows():
                    client_id = f"{emp_row[employee_id_code]} {emp_row[type_cat]}"
                    amounts.append({
                        'Amount': amount,
                        'DEBIT account': '',
                        'CREDIT account': emp_row[cred_account_recipient],
                        'Client Code': client_id,
                        'Campo': emp_row[recipient],
                        'GLFile': gl_file,
                        'Item Text': item_text
                    })
                # Skip standard entry for P+/N+ types
                continue
            else:
                amount = base_amount
                
            # Add standard entry
            amounts.append({
                'Amount': amount,
                'DEBIT account': debit_account,
                'CREDIT account': credit_account,
                'Client Code': client_code,
                'Campo': campo,
                'GLFile': gl_file,
                'Item Text': item_text
            })
    
    return pd.DataFrame(amounts)
