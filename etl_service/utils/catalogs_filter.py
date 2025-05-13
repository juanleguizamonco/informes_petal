
def catalogs_filter(df_catalog, catalog1_start, catalog1_end,catalog2_start, catalog2_end,
                    catalog3_start, catalog3_end,):
    # General catalogos format
    df_catalogs = df_catalog.copy().iloc[1:, :]
    df_index = df_catalogs.iloc[1, :]
    df_catalogs.columns = df_index
    df_catalogs.reset_index(drop=True, inplace=True)

    # Process catalog 1
    df_catalog1 = df_catalogs.iloc[2:, catalog1_start:catalog1_end]

    # Process catalog 2
    df_catalog2 = df_catalogs.iloc[2:, catalog2_start:catalog2_end].dropna()

    # Process catalog 3
    df_catalog3 = df_catalogs.iloc[2:, catalog3_start:catalog3_end]

    return(df_catalog1, df_catalog2, df_catalog3)