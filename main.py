from fastapi import FastAPI, HTTPException
import requests
import json

app = FastAPI(
    title="API RENIEC",
    description="Consulta datos del padrón RENIEC con índice optimizado",
    version="2.0.0"
)

# Archivos en Bunny
RENIEC_URL = "https://reniecdata.b-cdn.net/reniec.txt"
INDEX_URL = "https://reniecdata.b-cdn.net/index.json"

# Campos esperados en el archivo RENIEC
CAMPOS = [
    "DNI", "AP_PAT", "AP_MAT", "NOMBRES", "FECHA_NAC", "FCH_INSCRIPCION",
    "FCH_EMISION", "FCH_CADUCIDAD", "UBIGEO_NAC", "UBIGEO_DIR", "DIRECCION",
    "SEXO", "EST_CIVIL", "DIG_RUC", "MADRE", "PADRE"
]

# Cargamos el índice una sola vez al iniciar el servidor
print("📥 Descargando índice optimizado...")
try:
    index_response = requests.get(INDEX_URL)
    index_response.raise_for_status()
    INDEX = index_response.json()
    print(f"✅ Índice cargado con {len(INDEX):,} DNIs.")
except Exception as e:
    print(f"❌ Error al descargar índice: {e}")
    INDEX = {}

def procesar_linea(linea: str):
    """Convierte una línea de texto en un diccionario"""
    partes = linea.strip().split("|")
    return dict(zip(CAMPOS, partes))

def leer_linea_desde_bunny(offset: int):
    """Lee una línea desde Bunny usando Range Requests"""
    headers = {"Range": f"bytes={offset}-{offset + 2048}"}  # Leemos 2KB desde la posición exacta
    r = requests.get(RENIEC_URL, headers=headers)
    r.raise_for_status()
    return r.text.split("\n")[0]  # Primera línea en ese rango

def buscar_dni_en_bunny(dni: str):
    """Busca un DNI usando el índice"""
    if dni not in INDEX:
        return None

    offset = INDEX[dni]
    try:
        linea = leer_linea_desde_bunny(offset)
        if linea.startswith(dni + "|"):
            return procesar_linea(linea)
        return None
    except Exception:
        return None

def buscar_por_nombres_en_bunny(nombres: str, limit: int = 10):
    """Busca personas por nombres completos recorriendo Bunny en streaming"""
    resultados = []
    terminos = [t.strip().lower() for t in nombres.split() if t.strip()]

    if not terminos:
        return resultados

    with requests.get(RENIEC_URL, stream=True) as r:
        r.raise_for_status()
        buffer = ""
        for chunk in r.iter_content(chunk_size=1024 * 1024):  # 1MB por bloque
            buffer += chunk.decode("utf-8", errors="ignore")
            lineas = buffer.split("\n")
            buffer = lineas.pop()  # La última línea puede estar incompleta

            for linea in lineas:
                if not linea.strip():
                    continue
                linea_lower = linea.lower()
                if all(termino in linea_lower for termino in terminos):
                    persona = procesar_linea(linea)
                    resultados.append(persona)
                    if len(resultados) >= limit:
                        return resultados
    return resultados

@app.get("/dni/{dni}")
def buscar_dni(dni: str):
    """Endpoint para buscar un DNI"""
    if not dni.isdigit() or len(dni) != 8:
        raise HTTPException(status_code=400, detail="DNI inválido, debe tener 8 dígitos.")

    resultado = buscar_dni_en_bunny(dni)
    if resultado:
        return {"status": "ok", "data": resultado}
    else:
        raise HTTPException(status_code=404, detail="DNI no encontrado")

@app.get("/buscar")
def buscar_por_nombres(nombres: str, limit: int = 10):
    """Endpoint para buscar por nombres completos"""
    if not nombres or len(nombres.strip()) < 2:
        raise HTTPException(status_code=400, detail="Debe proporcionar al menos 2 caracteres para buscar.")

    if limit > 50:
        limit = 50  # Máximo 50 resultados

    resultados = buscar_por_nombres_en_bunny(nombres, limit)
    return {
        "status": "ok",
        "query": nombres,
        "total": len(resultados),
        "data": resultados
    }

@app.get("/")
def home():
    return {
        "mensaje": "API RENIEC funcionando ✅",
        "endpoints": [
            "/dni/{dni} - Buscar por DNI (rápido)",
            "/buscar?nombres={nombres} - Buscar por nombres completos"
        ]
    }

@app.get("/dni/{dni}")
async def buscar_dni(dni: str):
    """Endpoint para buscar por DNI."""
    if not dni.isdigit() or len(dni) != 8:
        raise HTTPException(status_code=400, detail="DNI inválido, debe tener 8 dígitos.")

    resultado = await buscar_dni_stream(dni)
    if resultado:
        return {"status": "ok", "data": resultado}
    else:
        raise HTTPException(status_code=404, detail="DNI no encontrado")

@app.get("/buscar")
async def buscar_por_nombres(nombres: str, limit: int = 10):
    """Endpoint para buscar personas por nombre."""
    if not nombres or len(nombres.strip()) < 2:
        raise HTTPException(status_code=400, detail="Debe proporcionar al menos 2 caracteres para buscar.")
    if limit > 50:
        limit = 50

    resultados = await buscar_por_nombres_stream(nombres, limit)
    return resultados
