import openpyxl
import datetime
import os

def gl_data_format(df1, df2, df3, storage_location, file_location, new_file_location, country, entity, 
                period, file_type, input_sheet, catalog_sheet, other_sheet1, other_sheet2, other_sheet3):
    """
    Transfers data from the input and catalogs sheets of the source Excel file to the output Excel file.
    """
    wb = openpyxl.load_workbook(file_location, keep_vba=True)
    sheet_input_in = wb[input_sheet]
    sheet_catalog = wb[catalog_sheet]

    ob = openpyxl.load_workbook(new_file_location, keep_vba=True)
    sheet_input_out = ob[input_sheet]
    sheet_catalog_out = ob[catalog_sheet]
    sheet_1 = ob[other_sheet1]
    sheet_2 = ob[other_sheet2]

    # File naming format with entity and period
    id_entidad = df1[entity].iloc[0]
    dat = datetime.now().strftime('%Y%m%d')
    periodo = df1[period].iloc[0]

    ruta = os.path.join(os.getcwd(), storage_location)
    carpeta = os.path.join(ruta, f"{file_type}_{country}_{id_entidad}_{periodo}_{dat}")

    if not os.path.exists(carpeta):
        os.makedirs(carpeta)

    s_filename = os.path.join(carpeta, f"{file_type}_{country}_{id_entidad}_{periodo}_{dat}.xlsm")

    # Paste df_detailed data into 'Detailed' sheet
    for r_idx, row in df2.iterrows():
        for c_idx, value in enumerate(row):
            sheet_1.cell(row=r_idx + 6, column=c_idx + 2, value=value)

    # Paste df_grouped data into 'Grouped' sheet
    for r_idx, row in df3.iterrows():
        for c_idx, value in enumerate(row):
            sheet_2.cell(row=r_idx + 6, column=c_idx + 2, value=value)

    # Copy column names and data from the 'Catalogos' sheet
    column_names_catalog = list(sheet_catalog.iter_rows(min_row=3, max_row=3, values_only=True))[0]
    for c_idx, value in enumerate(column_names_catalog):
        sheet_catalog_out.cell(row=3, column=c_idx + 1, value=value)
    for r_idx, row in enumerate(sheet_catalog.iter_rows(min_row=4, values_only=True)):
        for c_idx, value in enumerate(row):
            sheet_catalog_out.cell(row=r_idx + 4, column=c_idx + 1, value=value)

    # Copy column names and data from the 'Input' sheet
    column_names_input = list(sheet_input_in.iter_rows(min_row=3, max_row=3, values_only=True))[0]
    for c_idx, value in enumerate(column_names_input):
        sheet_input_out.cell(row=3, column=c_idx + 1, value=value)
    for r_idx, row in enumerate(sheet_input_in.iter_rows(min_row=4, values_only=True)):
        for c_idx, value in enumerate(row):
            sheet_input_out.cell(row=r_idx + 4, column=c_idx + 1, value=value)

    try:
        ob.save(s_filename)
    except Exception as e:
        print(f"Automatic save failed. Error: {e}")
        print(f"Please save the output file manually as: {file_type}_{country}_{id_entidad}_{periodo}_{dat}.xlsm")