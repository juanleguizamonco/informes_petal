import pandas as pd
import requests

def flatten_data(API_ENDPOINT, FILES_STORAGE):

    try:
        response = requests.get(API_ENDPOINT)
        response.raise_for_status()
        raw_data = response.json()

        # Aplanar los registros
        flattened_data = flatten_data(raw_data)
        df_in = pd.DataFrame(flattened_data)
        print("df_input loaded and flattened successfully from API")

    except Exception as e:
        print(f"Error loading or flattening df_input from API: {e}")
        df_in = pd.DataFrame()  # evitar errores downstream


# --- Load catalogs data from Excel using the defined storage path ---
    try:
        df_catalogos = pd.read_excel(
            FILES_STORAGE,
            sheet_name='Catalogs',
            engine='openpyxl',
            index_col=2
        )
        print("df_catalogos loaded successfully")
    except Exception as e:
        print(f"Error loading df_catalogos: {e}")
        df_catalogos = pd.DataFrame()

        return df_in, df_catalogos