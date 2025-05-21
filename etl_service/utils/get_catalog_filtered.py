from etl_service.utils.get_entity import get_entity
import pandas as pd

def get_catalog_filtered(
        catalog_1: pd.DataFrame, 
        df: pd.DataFrame, 
        period_ptype: str
) -> pd.DataFrame:
    
    company_list = get_entity(df)
    
    # Convertir la columna 'Company' a numérico (si es posible)
    catalog_1['Company'] = pd.to_numeric(catalog_1['Company'], errors='coerce')
    
    # Crear la columna 'Pivot look' en todo el DataFrame
    catalog_1['Pivot look'] = (
        catalog_1['Accounting type'].astype(str) + 
        catalog_1['Petal Code'].astype(str) + 
        catalog_1['Company'].astype(str) +
        catalog_1['Process Type'].astype(str) + 
        catalog_1['GLFile'].astype(str) + 
        catalog_1['Client Code'].astype(str)
    )
    
    # Filtrar por Company
    if 'ALL' in company_list:
        company_filter = catalog_1['Company'].isin(company_list) | catalog_1['Company'].isna()
    else:
        company_filter = catalog_1['Company'].isin(company_list)
    
    # Filtrar por Process Type
    if 'ALL' in period_ptype:
        process_type_filter = catalog_1['Process Type'].isin(period_ptype)
    else:
        process_type_filter = catalog_1['Process Type'].isin(period_ptype)
    
    # Aplicar los filtros
    df_filtrado = catalog_1[process_type_filter & company_filter]
    
    # Retornar los registros de df_catalogo_1 que tengan un 'Pivot look' presente en df_filtrado
    catalog1 = catalog_1[catalog_1['Pivot look'].isin(df_filtrado['Pivot look'].unique())]
    return  catalog1