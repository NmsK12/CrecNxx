from fastapi import FastAPI, HTTPException
import requests

app = FastAPI(
    title="API RENIEC",
    description="Consulta datos del padrón RENIEC en streaming desde Bunny Storage",
    version="1.0.0"
)

# URL de tu archivo en Bunny
RENIEC_URL = "https://reniecdata.b-cdn.net/reniec.txt"

# Campos esperados en el archivo RENIEC
CAMPOS = [
    "DNI", "AP_PAT", "AP_MAT", "NOMBRES", "FECHA_NAC", "FCH_INSCRIPCION",
    "FCH_EMISION", "FCH_CADUCIDAD", "UBIGEO_NAC", "UBIGEO_DIR", "DIRECCION",
    "SEXO", "EST_CIVIL", "DIG_RUC", "MADRE", "PADRE"
]

def procesar_linea(linea: str):
    """Convierte una línea de texto en un diccionario"""
    partes = linea.split("|")
    return dict(zip(CAMPOS, partes))

def buscar_dni_en_bunny(dni: str):
    """Busca el DNI en el archivo remoto usando streaming"""
    with requests.get(RENIEC_URL, stream=True) as r:
        r.raise_for_status()
        for linea in r.iter_lines():
            if linea:
                decoded = linea.decode("utf-8")
                if decoded.startswith(dni + "|"):
                    return procesar_linea(decoded)
    return None

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

@app.get("/")
def home():
    return {"mensaje": "API RENIEC funcionando ✅", "endpoints": ["/dni/{dni}"]}
