from fastapi import FastAPI, HTTPException
import requests
import logging

# -----------------------
# CONFIGURACIÓN DE LA API
# -----------------------
app = FastAPI(
    title="API RENIEC",
    description="Consulta datos del padrón RENIEC desde Bunny Storage",
    version="2.0.0"
)

# URL del archivo RENIEC en Bunny Storage
RENIEC_URL = "https://reniecdata.b-cdn.net/reniec.txt"

# Campos del archivo RENIEC
CAMPOS = [
    "DNI", "AP_PAT", "AP_MAT", "NOMBRES", "FECHA_NAC", "FCH_INSCRIPCION",
    "FCH_EMISION", "FCH_CADUCIDAD", "UBIGEO_NAC", "UBIGEO_DIR", "DIRECCION",
    "SEXO", "EST_CIVIL", "DIG_RUC", "MADRE", "PADRE"
]

# Tamaño aproximado de cada línea en bytes (ajústalo si es necesario)
LINE_SIZE = 150

# Configurar logs
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("API_RENIEC")

def procesar_linea(linea: str):
    """Convierte una línea en un diccionario"""
    partes = linea.split("|")
    return dict(zip(CAMPOS, partes))

def buscar_dni_en_bunny(dni: str):
    """Busca un DNI exacto usando Range Requests"""
    try:
        # Estimar la posición del DNI en bytes
        offset = int(dni) * LINE_SIZE

        # Descargamos solo un bloque pequeño alrededor de la posición estimada
        headers = {"Range": f"bytes={offset}-{offset + (LINE_SIZE * 5)}"}  # Lee 5 líneas desde ahí
        r = requests.get(RENIEC_URL, headers=headers, timeout=30)

        # Bunny debe responder con 206 (Partial Content)
        if r.status_code not in [200, 206]:
            logger.error(f"Error HTTP {r.status_code} al buscar DNI {dni}")
            return None

        # Procesamos el bloque
        buffer = r.content.decode("utf-8", errors="ignore")
        for linea in buffer.split("\n"):
            if linea.startswith(dni + "|"):
                return procesar_linea(linea)

        return None

    except Exception as e:
        logger.error(f"Error buscando DNI {dni}: {e}")
        return None

def buscar_por_nombres_en_bunny(nombres: str, limit: int = 10):
    """Busca personas por nombres completos (streaming, optimizado)"""
    resultados = []
    terminos = [t.strip().lower() for t in nombres.split() if t.strip()]

    if not terminos:
        return resultados

    try:
        # Descargamos en streaming para no consumir toda la RAM
        with requests.get(RENIEC_URL, stream=True, timeout=60) as r:
            r.raise_for_status()
            buffer = ""

            for chunk in r.iter_content(chunk_size=512 * 1024):  # 512 KB por bloque
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

    except Exception as e:
        logger.error(f"Error buscando nombres '{nombres}': {e}")
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
            "/dni/{dni} - Buscar por DNI",
            "/buscar?nombres={nombres} - Buscar por nombres completos"
        ]
    }
