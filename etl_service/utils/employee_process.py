from etl_service.utils.cost_centers import cost_centers
from etl_service.utils.amount_calc import amount_calculation
from etl_service.utils.row_creation import gl_row_creation
import pandas as pd


def employee_process(employee_data, df_catalog1, df_catalog2, df_catalog3, period, entity, pay_group, 
                     work_agreement, employee_name, employee_id):
    
    detailed_columns = [
    "Payroll Period", "Entity", "Pay group", "Work Agreement", "EE ID", "Employee Name",
    "Client ID", "Client Description", "Cost Center", "% CC", "Level2 (Optional)",
    "Level3 (Optional)", "Level4 (Optional)", "Amount", "Debt Account", "Acred Account",
    "Item Text", "GL File", "Observations"
    ]
    df_detailed = pd.DataFrame(columns=detailed_columns)
    
    analysis = []
    period_e = str(employee_data[period].iloc[0]) #ME03210.000-ISGDDMFNN
    entity_e = employee_data[entity].iloc[0]
    pay_group_e = (str(employee_data[pay_group].iloc[0])
                 if pay_group in employee_data.columns and not employee_data[pay_group].empty
                 else 'Paygroup not found')
    work_agreement_e = (str(employee_data[work_agreement].iloc[0])
                      if work_agreement in employee_data.columns and not employee_data[work_agreement].empty
                      else 'Work agreement not found')
    employee_name_e = (str(employee_data[employee_name].iloc[0])
                     if employee_name in employee_data.columns and not employee_data[employee_name].empty
                     else "")
    employee_id_e = int(employee_data[employee_id].values[0]) #ME01010.000-ISGDDMFNN
    
    centro_de_costos_prorrateo, centro_de_costos_solo = cost_centers(employee_data, df_catalog2)

    processed_keys = set()
    
    level2 = level3 = level4 = ''

    base_columns = ['PERIOD/Payroll Type', 'Legal','EMPLOYEE_ID','CO30720.000-IFGDNACNN','CO30730.000-IFGDNACNN','ME01010.000-ISGDDMFNN','ME01410.000-ISGDDMFNN','ME02010.000-SSGDDMFNN','ME41210.000-ISGDDMFNN']

    for paycode in employee_data.columns:

        if paycode in base_columns:
            continue

        value = employee_data.iloc[0][paycode]
        if pd.isna(value):
            continue

        # Verificar si el empleado está en centro_de_costos_prorrateo
        if any(cc[0] == employee_data[employee_id].iloc[0] for cc in centro_de_costos_prorrateo): #ME01010.000-ISGDDMFNN
            for centro_de_costos in centro_de_costos_prorrateo:
                percentage_cc = centro_de_costos[1]
                cost_center = centro_de_costos[2]  # Usar el CC correspondiente

                # Solo se procesan los paycodes que están en el DataFrame de base_amounts
                amounts = amount_calculation(value, percentage_cc, df_catalog1, paycode, employee_id, 
                                          df_catalog2, 'Petal Code','Accounting type', 'DEBIT account',
                                          'CREDIT account', 'Client Code', 'Campo', 'GLFile', 'Item TEXT', 
                                          'Employee ID', 'Tipo','BENEFICIARIA','NUM. ACREEDOR')

                # Añadir registros a la lista de análisis
                for _, amount_row in amounts.iterrows():
                    new_row = gl_row_creation(
                        period_e, entity_e, pay_group_e, work_agreement_e, employee_id_e, employee_name_e,
                        amount_row['Client Code'], amount_row['Campo'], cost_center, percentage_cc *100,
                        level2, level3, level4, amount_row['Amount'], amount_row['DEBIT account'], amount_row['CREDIT account'],
                        amount_row.get('Item Text', ''), amount_row['GLFile'])
                    analysis.append(new_row)

        else:  # Si no está en centro_de_costos_prorrateo, usar centro_de_costos_solo
            for centro_de_costos in centro_de_costos_solo:
                percentage_cc = centro_de_costos[1]
                cost_center = centro_de_costos[2]  # Usar el CC correspondiente

                # Crear una clave única para verificar duplicados
                key = (employee_data[employee_id], paycode, percentage_cc) #ME01010.000-ISGDDMFNN

                # Solo proceder si no hemos procesado esta combinación antes
                if key not in processed_keys:
                    processed_keys.add(key)  # Marcar como procesado

                    # Solo se procesan los paycodes que están en el DataFrame de base_amounts
                    amounts = amount_calculation(value, percentage_cc, df_catalog1, paycode, employee_id, 
                                          df_catalog2, 'Petal Code','Accounting type', 'DEBIT account',
                                          'CREDIT account', 'Client Code', 'Campo', 'GLFile', 'Item TEXT', 
                                          'Employee ID', 'Tipo','BENEFICIARIA','NUM. ACREEDOR')

                    # Añadir registros a la lista de análisis
                    for _, amount_row in amounts.iterrows():
                        new_row = gl_row_creation(
                            period_e, entity_e, pay_group_e, work_agreement_e, employee_id_e, employee_name_e,
                            amount_row['Client Code'], amount_row['Campo'], cost_center, percentage_cc * 100,
                            level2, level3, level4, amount_row['Amount'], amount_row['DEBIT account'], 
                            amount_row['CREDIT account'], amount_row.get('Item Text', ''), amount_row['GLFile']
                        )
                        analysis.append(new_row)
    
    df_analysis = pd.DataFrame(analysis)
    df_analysis = df_analysis[df_analysis['Amount'] != 0]
    df_detailed_return = pd.concat([df_detailed, df_analysis], ignore_index=True)

    return df_detailed_return
