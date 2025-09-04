from fastapi import FastAPI, HTTPException, Query
from typing import Optional, List, Any, Dict
import pandas as pd
import os
import gdown
import requests

TXT_PATH = "reniec.txt"        
DELIM    = "|"                 
ENCODING_DEFAULT = "utf-8"     

# URL de Google Drive (necesitarás proporcionar el enlace real)
GOOGLE_DRIVE_URL = "https://drive.usercontent.google.com/download?id=TU_ID_AQUI&export=download"

app = FastAPI(title="API RENIEC Simple con Pandas")
df_reniec = None

def download_reniec_data():
    """Descarga los datos de RENIEC desde Google Drive"""
    if os.path.exists(TXT_PATH):
        print("Archivo reniec.txt ya existe, usando archivo local")
        return True
    
    try:
        print("Descargando datos de RENIEC desde Google Drive...")
        print(f"URL: {GOOGLE_DRIVE_URL}")
        
        # Usar gdown con el enlace directo público
        try:
            gdown.download(GOOGLE_DRIVE_URL, TXT_PATH, quiet=False)
            print("Datos descargados exitosamente con gdown")
        except Exception as gdown_error:
            print(f"Gdown falló: {gdown_error}")
            print("Intentando con requests...")
            
            # Fallback con requests
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }
            
            response = requests.get(GOOGLE_DRIVE_URL, headers=headers, stream=True)
            response.raise_for_status()
            
            with open(TXT_PATH, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            
            print("Datos descargados exitosamente con requests")
        
        # Verificar que el archivo no sea HTML
        if os.path.exists(TXT_PATH):
            with open(TXT_PATH, 'r', encoding='utf-8', errors='ignore') as f:
                first_line = f.readline().strip()
                if first_line.startswith('<!DOCTYPE') or first_line.startswith('<html'):
                    print("ERROR: El archivo descargado es HTML, no TXT. El enlace requiere autenticación.")
                    os.remove(TXT_PATH)  # Eliminar archivo HTML
                    return False
        
        return True
    except Exception as e:
        print(f"Error al descargar los datos: {e}")
        print("Continuando sin datos...")
        return False

def load_data():
    """Carga los datos del archivo TXT"""
    global df_reniec
    try:
        # Primero intentar descargar los datos si no existen
        if not os.path.exists(TXT_PATH):
            if not download_reniec_data():
                print("No se pudieron descargar los datos, continuando sin datos")
                df_reniec = None
                return True
        
        print("Cargando datos en memoria...")
        # Leer más registros para encontrar datos
        df_reniec = pd.read_csv(
            TXT_PATH, 
            sep=DELIM,
            encoding=ENCODING_DEFAULT,
            nrows=100000,  # Aumentar límite para encontrar más datos
            on_bad_lines='skip',
            engine='python'
        )
        
        print(f"Datos cargados: {len(df_reniec)} registros")
        print(f"Columnas disponibles: {list(df_reniec.columns)}")
        
        # Limpiar valores NaN para JSON
        df_reniec = df_reniec.fillna('')
        
        return True
    except Exception as e:
        print(f"Error al cargar los datos: {e}")
        print("API funcionará sin datos")
        df_reniec = None
        return True

@app.on_event("startup")
def on_startup():
    load_data()

def _ensure_ready():
    if df_reniec is None:
        raise HTTPException(status_code=503, detail="Datos no cargados aún")

@app.get("/health")
def health():
    info = {
        "status": "ok" if df_reniec is not None else "error",
        "data_loaded": df_reniec is not None,
        "txt_exists": os.path.exists(TXT_PATH),
        "txt_path": TXT_PATH,
    }
    try:
        if os.path.exists(TXT_PATH):
            info["txt_size_bytes"] = os.path.getsize(TXT_PATH)
        if df_reniec is not None:
            info["rows_loaded"] = len(df_reniec)
    except Exception as e:
        info["health_error"] = str(e)
    return info

@app.get("/")
def root():
    return {
        "message": "API RENIEC funcionando correctamente",
        "status": "ok",
        "data_loaded": df_reniec is not None,
        "total_records": len(df_reniec) if df_reniec is not None else 0,
        "endpoints": [
            "/dni/{dni} - Búsqueda por DNI",
            "/nombres/{nombres} - Búsqueda por nombres completos",
            "/apellido/{apellido} - Búsqueda por apellido (paterno o materno)",
            "/health - Estado de la API"
        ]
    }

@app.get("/dni/{dni}")
def get_persona(dni: str):
    """Búsqueda por DNI"""
    _ensure_ready()
    
    result = df_reniec[df_reniec['DNI'].astype(str) == str(dni)]
    
    if result.empty:
        raise HTTPException(status_code=404, detail="No encontrado")
    
    return result.iloc[0].to_dict()

@app.get("/nombres/{nombres}")
def buscar_por_nombres(
    nombres: str,
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
):
    """Búsqueda por nombres completos"""
    _ensure_ready()
    
    # Limpiar y preparar el término de búsqueda
    nombres_clean = nombres.replace('_', ' ').replace('-', ' ').strip().upper()
    search_terms = [term.strip() for term in nombres_clean.split() if term.strip()]
    
    if not search_terms:
        return []
    
    # Búsqueda mejorada: buscar cada término en cualquier columna de nombres
    conditions = []
    
    for term in search_terms:
        term_conditions = []
        for col in ['NOMBRES', 'AP_PAT', 'AP_MAT']:
            if col in df_reniec.columns:
                term_conditions.append(df_reniec[col].str.upper().str.contains(term, na=False))
        # Al menos una columna debe contener este término
        if term_conditions:
            conditions.append(pd.concat(term_conditions, axis=1).any(axis=1))
    
    # Todos los términos deben estar presentes
    if conditions:
        result = df_reniec[pd.concat(conditions, axis=1).all(axis=1)]
    else:
        result = df_reniec.iloc[0:0]  # DataFrame vacío
    
    if result.empty:
        # Si no encuentra con búsqueda completa, intentar búsqueda parcial
        nombres_upper = nombres_clean
        partial_conditions = []
        for col in ['NOMBRES', 'AP_PAT', 'AP_MAT']:
            if col in df_reniec.columns:
                partial_conditions.append(df_reniec[col].str.upper().str.contains(nombres_upper, na=False))
        
        if partial_conditions:
            result = df_reniec[pd.concat(partial_conditions, axis=1).any(axis=1)]
    
    # Aplicar paginación
    result = result.iloc[offset:offset+limit]
    
    return result.to_dict('records')

@app.get("/apellido/{apellido}")
def buscar_por_apellido(
    apellido: str,
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
):
    """Búsqueda por apellido (paterno o materno)"""
    _ensure_ready()
    
    # Buscar en apellido paterno y materno
    result = df_reniec[
        (df_reniec['AP_PAT'].str.contains(apellido, case=False, na=False)) |
        (df_reniec['AP_MAT'].str.contains(apellido, case=False, na=False))
    ]
    
    # Aplicar paginación
    result = result.iloc[offset:offset+limit]
    
    return result.to_dict('records')

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
