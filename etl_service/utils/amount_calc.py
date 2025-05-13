import pandas as pd

def amount_calculation(value, percentage_cc, catalog, paycode, employee_id, catalog2, petal_code,account_type, 
                    deb_account,cred_account, cli_code, camp, file_gl, item, employee_id_code, type_cat,
                    recipient, cred_account_recipient):

    paycode_str = str(paycode).strip()
  
    catalog[petal_code] = catalog[petal_code].astype(str).str.strip()#'Petal Code'
    filas_catalogo = catalog[catalog[petal_code] == paycode_str]
    
    if filas_catalogo.empty:
        return pd.DataFrame()
    
    base_amount = abs(float(value) * percentage_cc)
    amounts = []
    for _, fila in filas_catalogo.iterrows():
        accounting_type = fila.get(account_type)#'Accounting type'
        debit_account = fila.get(deb_account)#'DEBIT account'
        credit_account = fila.get(cred_account)#'CREDIT account'
        client_code = fila.get(cli_code)#'Client Code'
        campo = fila.get(camp)#'Campo'
        gl_file = fila.get(file_gl)#'GLFile'
        item_text = fila.get(item)#'Item TEXT'
        
        if pd.notna(debit_account) or pd.notna(credit_account):
            if accounting_type in ['D-', 'C+']:
                amount = -base_amount
            elif accounting_type in ['P+', 'N+']:
                amount = base_amount
                df_temp = catalog2[catalog2[employee_id_code] == employee_id] #'Employee ID'
                print(f"Catalog matches in df_catalogo3 for Employee ID {employee_id}: {len(df_temp)}")
                for _, row3 in df_temp.iterrows():
                    client_id = f"{row3[employee_id_code]} {row3[type_cat]}" #ME01010.000-ISGDDMFNN
                    client_description = row3[recipient]
                    acred_account = row3[cred_account_recipient]
                    amounts.append({
                        'Amount': amount,
                        'DEBIT account': '', 
                        'CREDIT account': acred_account,
                        'Client Code': client_id,
                        'Campo': client_description,
                        'GLFile': gl_file,
                        'Item Text': item_text
                    })
                continue
            else:
                amount = base_amount
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
