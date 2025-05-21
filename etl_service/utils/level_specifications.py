import pandas as pd
from typing import Dict, Any, Optional

def get_level_specifications(df_catalog3: pd.DataFrame) -> Dict[str, Optional[Any]]:
    """Extract level specifications from catalog3."""
    levels = {}
    
    # Get Level1
    filtered1 = df_catalog3.loc[df_catalog3['Especificacion'] == 'Level1', 'Paycode Input']
    levels['level1'] = filtered1.iloc[0] if not filtered1.empty else None
    
    # Get Level2
    filtered2 = df_catalog3.loc[df_catalog3['Especificacion'] == 'Level2 (Optional)', 'Paycode Input']
    levels['level2'] = filtered2.iloc[0] if not filtered2.empty else None
    
    # Get Level3
    filtered3 = df_catalog3.loc[df_catalog3['Especificacion'] == 'Level3 (Optional)', 'Paycode Input']
    levels['level3'] = filtered3.iloc[0] if not filtered3.empty else None
    
    # Get Level4
    filtered4 = df_catalog3.loc[df_catalog3['Especificaciones'] == 'Level4 (Optional)', 'Paycode Input']
    levels['level4'] = filtered4.iloc[0] if not filtered4.empty else None
    
    return levels