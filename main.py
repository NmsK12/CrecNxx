import requests
from fastapi import FastAPI, HTTPException

app = FastAPI()

BASE_INDEX_URL = "https://reniecdata.b-cdn.net/indices"
RENIEC_URL = "https://reniecdata.b-cdn.net/reniec/reniec.txt"

def cargar_indice_prefijo(prefijo: str):
    url = f"{BASE_INDEX_URL}/{prefijo}.json"
    print(f"[DEBUG] Descargando índice: {url}")

    try:
        r = requests.get(url, timeout=10)
        r.raise_for_status()

        try:
            data = r.json()
            print(f"[DEBUG] Índice {prefijo}.json cargado con {len(data)} registros")
            return data
        except ValueError:
            print(f"[ERROR] El archivo {prefijo}.json no es un JSON válido")
            return {}

    except requests.exceptions.RequestException as e:
        print(f"[ERROR] No se pudo descargar el índice {prefijo}: {e}")
        return {}

def buscar_dni_en_bunny(dni: str):
    prefijo = dni[:2]
    print(f"\n[INFO] Buscando DNI {dni} en prefijo {prefijo}...")

    # Descargar índice
    index = cargar_indice_prefijo(prefijo)

    if not index:
        print(f"[WARN] No se encontró índice para prefijo {prefijo}")
        return None

    if dni not in index:
        print(f"[WARN] DNI {dni} no encontrado en el índice {prefijo}.json")
        return None

    # Obtener offset
    offset = index[dni]
    print(f"[DEBUG] Offset encontrado para {dni}: {offset}")

    # Descargar datos desde reniec.txt usando Range
    headers = {"Range": f"bytes={offset}-{offset + 2048}"}
    try:
        r = requests.get(RENIEC_URL, headers=headers, timeout=10)
        r.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"[ERROR] Error descargando línea del TXT: {e}")
        return None

    # Leer la primera línea de la respuesta
    linea = r.text.split("\n")[0]
    print(f"[DEBUG] Línea obtenida: {linea}")

    # Validar si la línea coincide con el DNI
    if not linea.startswith(dni + "|"):
        print(f"[ERROR] La línea no coincide con el DNI {dni}")
        return None

    return procesar_linea(linea)

def procesar_linea(linea: str):
    partes = linea.split("|")
    return {
        "dni": partes[0],
        "nombre": partes[1],
        "apellido_paterno": partes[2],
        "apellido_materno": partes[3],
        "departamento": partes[4],
        "provincia": partes[5],
        "distrito": partes[6]
    }

@app.get("/")
def home():
    return {"status": "ok", "mensaje": "API RENIEC funcionando ✅"}

@app.get("/dni/{dni}")
def get_dni(dni: str):
    if not dni.isdigit() or len(dni) != 8:
        raise HTTPException(status_code=400, detail="DNI inválido")

    resultado = buscar_dni_en_bunny(dni)
    if resultado is None:
        raise HTTPException(status_code=404, detail="DNI no encontrado")

    return resultado
