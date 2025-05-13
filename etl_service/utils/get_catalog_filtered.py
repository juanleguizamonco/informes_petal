from etl_service.utils.get_entity import get_entity
import pandas as pd

def get_catalog_filtered(df_catalog_1, df, period_ptype):
    company_list = get_entity(df)
    
    # Convertir la columna 'Company' a numérico (si es posible)
    df_catalog_1['Company'] = pd.to_numeric(df_catalog_1['Company'], errors='coerce')
    
    # Crear la columna 'Pivot look' en todo el DataFrame
    df_catalog_1['Pivot look'] = (
        df_catalog_1['Accounting type'].astype(str) + 
        df_catalog_1['Petal Code'].astype(str) + 
        df_catalog_1['Company'].astype(str) +
        df_catalog_1['Process Type'].astype(str) + 
        df_catalog_1['GLFile'].astype(str) + 
        df_catalog_1['Client Code'].astype(str)
    )
    
    # Filtrar por Company
    if 'ALL' in company_list:
        company_filter = df_catalog_1['Company'].isin(company_list) | df_catalog_1['Company'].isna()
    else:
        company_filter = df_catalog_1['Company'].isin(company_list)
    
    # Filtrar por Process Type
    if 'ALL' in period_ptype:
        process_type_filter = df_catalog_1['Process Type'].isin(period_ptype)
    else:
        process_type_filter = df_catalog_1['Process Type'].isin(period_ptype)
    
    # Aplicar los filtros
    df_filtrado = df_catalog_1[process_type_filter & company_filter]
    
    # Retornar los registros de df_catalogo_1 que tengan un 'Pivot look' presente en df_filtrado
    df_catalog1 = df_catalog_1[df_catalog_1['Pivot look'].isin(df_filtrado['Pivot look'].unique())]
    return  df_catalog1