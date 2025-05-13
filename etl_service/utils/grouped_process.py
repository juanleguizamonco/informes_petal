import pandas as pd
import numpy as np

def grouped_process(analysis):
    df_analysis = pd.DataFrame(analysis)
    df_analysis = df_analysis[df_analysis['Amount'] != 0]
    df_detailed = pd.concat([df_detailed, df_analysis], ignore_index=True)

    df_detailed_copy = df_detailed.copy()

    grouped_columns = [
        "Payroll Period", "Entity", "Pay group", "Client ID", "Client Description", "Cost Center", "Level2 (Optional)",
        "Level3 (Optional)", "Level4 (Optional)", "Amount", "Debt Account", "Acred Account", "Item Text", "GL File", 
        "Observations"
    ]
    df_grouped = pd.DataFrame(columns=grouped_columns)

    # Merge accounts and clean empty cells
    df_detailed_copy["Debt Account"] = df_detailed_copy["Debt Account"].replace("", np.nan)
    df_detailed_copy["Acred Account"] = df_detailed_copy["Acred Account"].replace("", np.nan)
    df_detailed_copy["Account"] = df_detailed_copy["Debt Account"].combine_first(df_detailed_copy["Acred Account"])
    df_detailed_copy["Account Type"] = np.where(
        df_detailed_copy["Debt Account"].notna(), "D+",
        np.where(df_detailed_copy["Acred Account"].notna(), "C+", None)
    )

    # Group and aggregate the detailed data
    df_grouped_final = df_detailed_copy.groupby(
        ['Payroll Period', 'Entity', 'Pay group', "Client ID", "Client Description", 
        "Cost Center", "Account", "Item Text", "GL File"],
        dropna=False,
        as_index=False
    ).agg({
        "Level2 (Optional)": 'sum',
        "Level3 (Optional)": 'sum',
        "Level4 (Optional)": 'sum',
        "Amount": 'sum',
        "Account Type": lambda x: x.dropna().iloc[0] if not x.dropna().empty else None
    })

    df_grouped_final["Debt Account"] = np.where(df_grouped_final["Account Type"] == "D+", 
                                                df_grouped_final["Account"], '')
    df_grouped_final["Acred Account"] = np.where(df_grouped_final["Account Type"] == "C+", 
                                                df_grouped_final["Account"], '')

    df_grouped_final = df_grouped_final[['Payroll Period','Entity','Pay group','Client ID','Client Description',
                                        'Cost Center','Level2 (Optional)','Level3 (Optional)','Level4 (Optional)',
                                        'Amount','Debt Account','Acred Account','Item Text','GL File']]

    df_grouped_return = pd.concat([df_grouped, df_grouped_final])

    return df_grouped_return