def get_entity(data_frame, code):
    """Returns a list of unique entities from the input DataFrame.
    Also requieres the code to aply the filter"""
    #'AY01200.000-ISGDDMFNN'
    return data_frame[code].dropna().unique().tolist()
