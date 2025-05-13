from fastapi import FastAPI, HTTPException
from typing import List, Dict, Any
from api_service.routers import router as api_router
from fastapi.middleware.cors import CORSMiddleware
import json
import os

app = FastAPI(title="Payroll API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Personalízalo para producción
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(api_router)


#Revisar
DATA_FILE = os.path.join(os.path.dirname(__file__), "Ejemplo calculo.json")

def flatten_record(record: Dict[str, Any]) -> Dict[str, Any]:
    flat = {}
    for key, val in record.items():
        if isinstance(val, dict):
            for subk, subv in val.items():
                flat[f"{key}__{subk}"] = subv
        else:
            flat[key] = val
    return flat

@app.post("/api/raw-data", response_model=List[Dict[str, Any]])
async def get_raw_data():
    try:
        with open(DATA_FILE, "r", encoding="utf-8") as f:
            raw_items = json.load(f)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error leyendo {DATA_FILE}: {e}")

    try:
        flattened = [flatten_record(item) for item in raw_items]
        return flattened
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error procesando datos: {e}")