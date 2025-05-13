def process_input(df_input, employee_id='ME01010.000-ISGDDMFNN'):
    for employee in df_input[employee_id].unique():# ME01010.000-ISGDDMFNN
        employee_data = df_input[df_input[employee_id] == employee].copy() #ME01010.000-ISGDDMFNN
    return employee_data
   