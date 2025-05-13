from fastapi import APIRouter, HTTPException
from typing import List, Dict, Any
import json
from pathlib import Path
from api_service.services.flattener import flatten_data

router = APIRouter()

# Ruta al archivo local JSON (en una carpeta data/)
DATA_FILE = Path(__file__).parent.parent / "data" / "Ejemplo_calculo.json"

@router.post("/raw-data", response_model=List[Dict[str, Any]])
async def get_raw_data():
    """
    Lee el JSON local y lo devuelve aplanado como lista de dicts.
    """
    try:
        with open(DATA_FILE, "r", encoding="utf-8") as f:
            raw_json = json.load(f)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error leyendo JSON: {e}")

    try:
        flattened = flatten_data(raw_json)
        return flattened
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error procesando JSON: {e}")