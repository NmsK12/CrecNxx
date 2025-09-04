from fastapi import FastAPI, HTTPException
import httpx
import logging

# -----------------------
# CONFIGURACIÓN DE LA API
# -----------------------
app = FastAPI(
    title="API RENIEC",
    description="Consulta datos del padrón RENIEC directamente desde Bunny Storage",
    version="2.0.0"
)

# URL de tu archivo en Bunny CDN
RENIEC_URL = "https://reniecdata.b-cdn.net/reniec.txt"

# Definir los campos esperados en cada línea del TXT
CAMPOS = [
    "DNI", "AP_PAT", "AP_MAT", "NOMBRES", "FECHA_NAC", "FCH_INSCRIPCION",
    "FCH_EMISION", "FCH_CADUCIDAD", "UBIGEO_NAC", "UBIGEO_DIR", "DIRECCION",
    "SEXO", "EST_CIVIL", "DIG_RUC", "MADRE", "PADRE"
]

# Configurar logs
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("API_RENIEC")

def procesar_linea(linea: str):
    """Convierte una línea de texto en un diccionario."""
    partes = linea.split("|")
    return dict(zip(CAMPOS, partes))

async def buscar_dni_stream(dni: str):
    """Busca un DNI dentro de reniec.txt usando streaming para no descargar todo el archivo."""
    async with httpx.AsyncClient() as client:
        async with client.stream("GET", RENIEC_URL) as response:
            if response.status_code == 404:
                raise HTTPException(status_code=404, detail="Base de datos no encontrada en Bunny Storage.")
            if response.status_code != 200:
                raise HTTPException(status_code=500, detail=f"Error al acceder a Bunny: {response.status_code}")

            buffer = ""
            async for chunk in response.aiter_bytes():
                buffer += chunk.decode("utf-8", errors="ignore")
                lineas = buffer.split("\n")
                buffer = lineas.pop()

                for linea in lineas:
                    if linea.startswith(dni + "|"):
                        return procesar_linea(linea)
    return None

async def buscar_por_nombres_stream(nombres: str, limit: int = 10):
    """Busca personas por nombre dentro de reniec.txt usando streaming para no descargar todo el archivo."""
    terminos = [t.strip().lower() for t in nombres.split() if t.strip()]
    resultados = []

    if not terminos:
        return resultados

    async with httpx.AsyncClient() as client:
        async with client.stream("GET", RENIEC_URL) as response:
            if response.status_code == 404:
                raise HTTPException(status_code=404, detail="Base de datos no encontrada en Bunny Storage.")
            if response.status_code != 200:
                raise HTTPException(status_code=500, detail=f"Error al acceder a Bunny: {response.status_code}")

            buffer = ""
            async for chunk in response.aiter_bytes():
                buffer += chunk.decode("utf-8", errors="ignore")
                lineas = buffer.split("\n")
                buffer = lineas.pop()

                for linea in lineas:
                    if not linea.strip():
                        continue

                    linea_lower = linea.lower()
                    if all(termino in linea_lower for termino in terminos):
                        persona = procesar_linea(linea)
                        resultados.append(persona)

                        if len(resultados) >= limit:
                            return {
                                "status": "ok",
                                "query": nombres,
                                "total": len(resultados),
                                "data": resultados
                            }

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
