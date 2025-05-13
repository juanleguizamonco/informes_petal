def gl_row_creation(period, entity, pay_group, work_agreement, employee_id, employee_name,
               client_id, client_description, cost_center, percentage_cc,
               level2, level3, level4, amount, debit_account, acred_account,
               item_text, gl_file):
    """Creates a dictionary representing a row for the analysis DataFrame."""
    return {
        'Payroll Period': period,
        "Entity": entity,
        "Pay group": pay_group,
        "Work Agreement": work_agreement,
        "EE ID": employee_id,
        "Employee Name": employee_name,
        "Client ID": client_id,
        "Client Description": client_description,
        "Cost Center": cost_center,
        "% CC": percentage_cc,
        "Level2 (Optional)": level2,
        "Level3 (Optional)": level3,
        "Level4 (Optional)": level4,
        "Amount": amount,
        "Debt Account": debit_account,
        "Acred Account": acred_account,
        "Item Text": item_text,
        "GL File": gl_file,
        "Observations": ""
    }