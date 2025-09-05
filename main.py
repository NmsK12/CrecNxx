import requests
from collections import OrderedDict
from fastapi import FastAPI, HTTPException
from typing import Dict, Any, List

app = FastAPI(title="API RENIEC - DNI + Nombre")

# --- Config ---
BASE_INDEX_URL = "https://reniecdata.b-cdn.net/indices"
RENIEC_URL = "https://reniecdata.b-cdn.net/reniec.txt"
INDEX_CACHE_SIZE = 3

# Campos reales en reniec.txt
CAMPOS = [
    "dni", "apellido_paterno", "apellido_materno", "nombres",
    "fecha_nacimiento", "fecha_inscripcion", "fecha_emision", "fecha_caducidad",
    "ubigeo_nacimiento", "ubigeo_direccion", "direccion",
    "sexo", "estado_civil", "digito_ruc", "madre", "padre"
]

_index_cache: "OrderedDict[str, Dict[str, int]]" = OrderedDict()


# -----------------------
# Cache de índices
# -----------------------
def _cache_put(prefijo: str, data: Dict[str, int]) -> None:
    if prefijo in _index_cache:
        _index_cache.move_to_end(prefijo)
        _index_cache[prefijo] = data
        return
    _index_cache[prefijo] = data
    if len(_index_cache) > INDEX_CACHE_SIZE:
        _index_cache.pop(next(iter(_index_cache)))


def cargar_indice_prefijo(prefijo: str) -> Dict[str, int]:
    if prefijo in _index_cache:
        return _index_cache[prefijo]

    url = f"{BASE_INDEX_URL}/{prefijo}.json"
    try:
        print(f"[DEBUG] Descargando índice: {url}")
        r = requests.get(url, timeout=15)
        r.raise_for_status()
        data = r.json()
        cleaned = {str(k): int(v) for k, v in data.items()}
        _cache_put(prefijo, cleaned)
        return cleaned
    except:
        return {}


# -----------------------
# Procesamiento de línea
# -----------------------
def procesar_linea(linea: str) -> Dict[str, Any]:
    partes = linea.split("|")
    if len(partes) < len(CAMPOS):
        partes += [""] * (len(CAMPOS) - len(partes))
    partes = [p.strip() for p in partes[:len(CAMPOS)]]
    return dict(zip(CAMPOS, partes))


# -----------------------
# Búsqueda por DNI
# -----------------------
def buscar_dni_en_bunny(dni: str) -> Dict[str, Any] | None:
    prefijo = dni[:2]
    index = cargar_indice_prefijo(prefijo)
    if not index or dni not in index:
        return None

    offset = index[dni]
    headers = {"Range": f"bytes={offset}-{offset + 4096}"}
    try:
        r = requests.get(RENIEC_URL, headers=headers, timeout=15)
        r.raise_for_status()
    except:
        return None

    text = r.text
    if text.startswith(dni + "|"):
        linea = text.split("\n", 1)[0]
    else:
        idx = text.find("\n" + dni + "|")
        if idx != -1:
            remaining = text[idx + 1 :]
            linea = remaining.split("\n", 1)[0]
        else:
            return None
    return procesar_linea(linea)


# -----------------------
# Búsqueda por nombres
# -----------------------
def buscar_por_nombres_en_bunny(nombres: str, limit: int = 10) -> List[Dict[str, Any]]:
    resultados: List[Dict[str, Any]] = []
    terminos = [t.strip().lower() for t in nombres.split() if t.strip()]
    if not terminos:
        return resultados

    try:
        with requests.get(RENIEC_URL, stream=True, timeout=60) as r:
            r.raise_for_status()
            buffer = ""
            for chunk in r.iter_content(chunk_size=512 * 1024):
                if not chunk:
                    continue
                buffer += chunk.decode("utf-8", errors="ignore")
                lines = buffer.split("\n")
                buffer = lines.pop()
                for line in lines:
                    if not line:
                        continue
                    linea_lower = line.lower()
                    if all(term in linea_lower for term in terminos):
                        resultados.append(procesar_linea(line))
                        if len(resultados) >= limit:
                            return resultados
    except:
        return resultados

    return resultados


# -----------------------
# Endpoints
# -----------------------
@app.get("/")
def home():
    return {"status": "ok", "message": "API RENIEC funcionando ✅"}


@app.get("/dni/{dni}")
def endpoint_dni(dni: str):
    if not dni.isdigit() or len(dni) != 8:
        raise HTTPException(status_code=400, detail="DNI inválido")
    result = buscar_dni_en_bunny(dni)
    if result is None:
        raise HTTPException(status_code=404, detail="DNI no encontrado")
    return result


@app.get("/buscar")
def endpoint_buscar(nombres: str, limit: int = 10):
    if not nombres or len(nombres.strip()) < 2:
        raise HTTPException(status_code=400, detail="Proporciona un nombre válido")
    if limit < 1:
        limit = 1
    if limit > 50:
        limit = 50
    resultados = buscar_por_nombres_en_bunny(nombres, limit)
    return {"status": "ok", "query": nombres, "total": len(resultados), "data": resultados}


@app.get("/nombre/{nombre}")
def endpoint_nombre(nombre: str, limit: int = 10):
    if len(nombre.strip()) < 2:
        raise HTTPException(status_code=400, detail="Proporciona un nombre válido")
    if limit < 1:
        limit = 1
    if limit > 50:
        limit = 50
    resultados = buscar_por_nombres_en_bunny(nombre, limit)
    if not resultados:
        raise HTTPException(status_code=404, detail="No se encontraron resultados")
    return {"status": "ok", "query": nombre, "total": len(resultados), "data": resultados}
