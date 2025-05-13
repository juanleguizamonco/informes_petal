def nan_elimination(cadena):
    """Removes 'nan' strings from the input string."""
    if isinstance(cadena, str):
        return ', '.join(filter(lambda x: x.lower() != 'nan', cadena.split(', ')))
    return cadena