def get_period(data_frame, code):
    """Extracts and returns the payroll period from the input DataFrame."""
    period_data = data_frame[code].str.split(' ').str[1].dropna().unique() #ME03210.000-ISGDDMFNN
    if len(period_data) == 1:
        return [str(period_data[0])]
    elif len(period_data) > 1:
        raise ValueError("Multiple periods found; expected only one.")
    return []