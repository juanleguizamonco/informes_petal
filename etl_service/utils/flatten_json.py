import pandas as pd

def flatten_json(df_in):
   
    df_flat = pd.json_normalize(df_in, sep='_')
    df_flat = df_flat.applymap(lambda x: x.strip() if isinstance(x, str) else x)
    df_clean = df_flat.replace({0: pd.NA, 0.0: pd.NA, "": pd.NA, "NaN": pd.NA})
    df_input = df_clean.dropna(axis=1, how='all').iloc[:, 6:]

    return df_input