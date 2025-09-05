import requests
import json
from fastapi import FastAPI, HTTPException

app = FastAPI()

BASE_INDEX_URL = "https://reniecdata.b-cdn.net/reniec/indices"
RENIEC_URL = "https://reniecdata.b-cdn.net/reniec/reniec.txt"

def cargar_indice_prefijo(prefijo: str):
    """Descarga solo el índice de un prefijo de dos dígitos"""
    url = f"{BASE_INDEX_URL}/{prefijo}.json"
    try:
        r = requests.get(url, timeout=10)
        r.raise_for_status()
        return r.json()
    except requests.exceptions.HTTPError:
        return {}  # Si no existe ese índice, devolvemos vacío

def buscar_dni_en_bunny(dni: str):
    """Busca un DNI usando el índice por prefijo"""
    prefijo = dni[:2]

    # Cargar solo el índice necesario
    index = cargar_indice_prefijo(prefijo)
    if not index or dni not in index:
        return None

    # Obtener el offset del DNI
    offset = index[dni]

    # Hacer una solicitud parcial al TXT usando Range
    headers = {"Range": f"bytes={offset}-{offset + 2048}"}
    r = requests.get(RENIEC_URL, headers=headers, timeout=10)
    r.raise_for_status()

    # Leer la línea correspondiente
    linea = r.text.split("\n")[0]
    return procesar_linea(linea) if linea.startswith(dni + "|") else None

def procesar_linea(linea: str):
    """Convierte una línea del TXT en JSON"""
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

@app.get("/dni/{dni}")
def get_dni(dni: str):
    """Endpoint para buscar DNI"""
    if not dni.isdigit() or len(dni) != 8:
        raise HTTPException(status_code=400, detail="DNI inválido")

    resultado = buscar_dni_en_bunny(dni)
    if resultado is None:
        raise HTTPException(status_code=404, detail="DNI no encontrado")

    return resultado
