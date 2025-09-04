from fastapi import FastAPI, HTTPException, Query
from typing import Optional, List, Any, Dict
import duckdb, os
import gdown
import requests

TXT_PATH = "reniec.txt"        
DB_PATH  = "personas.duckdb"   
DELIM    = "|"                 
ENCODING_DEFAULT = "utf-8"     

# URL de Google Drive (archivo completo de 5.8GB)
GOOGLE_DRIVE_URL = "https://drive.usercontent.google.com/download?id=12OYjI-Z6yOMCMCU4kCIObliXHHi7s98T&export=download&authuser=0&confirm=t&uuid=bcd21b93-b050-4af7-b349-e195c2b75cc2&at=AN8xHopYN2rm6zpH2Dj6ozcmqz06%3A1756957395612"

app = FastAPI(title="API RENIEC con DuckDB")
STARTUP_ERROR: Optional[str] = None

def download_reniec_data():
    """Descarga los datos de RENIEC desde Google Drive"""
    if os.path.exists(TXT_PATH):
        file_size = os.path.getsize(TXT_PATH)
        print(f"Archivo reniec.txt ya existe, tamaño: {file_size / (1024**3):.2f} GB")
        # Si el archivo existe y es mayor a 1GB, asumimos que está completo
        if file_size > 1024**3:
            print("Archivo parece estar completo, usando archivo local")
            return True
        else:
            print("Archivo es muy pequeño, volviendo a descargar...")
            os.remove(TXT_PATH)

    try:
        print("Descargando datos de RENIEC desde Google Drive...")
        print(f"URL: {GOOGLE_DRIVE_URL}")

        # Intentar primero con requests para mejor control
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }

        print("Intentando descarga con requests...")
        response = requests.get(GOOGLE_DRIVE_URL, headers=headers, stream=True)
        response.raise_for_status()

        # Verificar el tamaño del contenido si está disponible
        total_size = int(response.headers.get('content-length', 0))
        if total_size > 0:
            print(f"Tamaño esperado: {total_size / (1024**3):.2f} GB")

        downloaded_size = 0
        with open(TXT_PATH, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
                    downloaded_size += len(chunk)

        actual_size = os.path.getsize(TXT_PATH)
        print(f"Tamaño descargado: {actual_size / (1024**3):.2f} GB")

        # Verificar que no sea un archivo HTML de error
        if os.path.exists(TXT_PATH):
            with open(TXT_PATH, 'r', encoding='utf-8', errors='ignore') as f:
                first_line = f.readline().strip()
                if first_line.startswith('<!DOCTYPE') or first_line.startswith('<html'):
                    print("ERROR: El archivo descargado es HTML, no TXT. El enlace requiere autenticación.")
                    print(f"Contenido del archivo: {first_line[:200]}...")
                    os.remove(TXT_PATH)
                    return False

        # Verificar que el archivo tenga un tamaño razonable (> 1GB)
        if actual_size < 1024**3:  # Menos de 1GB
            print(f"ERROR: Archivo descargado es muy pequeño ({actual_size / (1024**3):.2f} GB). Se esperaba al menos 5GB.")
            print("El enlace podría estar apuntando a un archivo incorrecto.")
            if os.path.exists(TXT_PATH):
                os.remove(TXT_PATH)
            return False

        print("Datos descargados exitosamente")
        return True

    except Exception as e:
        print(f"Error al descargar los datos: {e}")
        if os.path.exists(TXT_PATH):
            os.remove(TXT_PATH)
        print("Continuando sin datos...")
        return False

def _build_db(txt_mtime: Optional[int], existing_con: Optional[duckdb.DuckDBPyConnection]=None):
    if not os.path.exists(TXT_PATH):
        raise FileNotFoundError(f"No se encontró el archivo TXT: {TXT_PATH}")

    con = existing_con or duckdb.connect(DB_PATH)
    con.execute("CREATE TABLE IF NOT EXISTS meta(k TEXT PRIMARY KEY, v BIGINT)")
    con.execute("DROP VIEW IF EXISTS personas_v")
    con.execute("DROP TABLE IF EXISTS personas")

    columns = {
        "DNI": "VARCHAR",
        "AP_PAT": "VARCHAR",
        "AP_MAT": "VARCHAR",
        "NOMBRES": "VARCHAR",
        "FECHA_NAC": "VARCHAR",
        "FCH_INSCRIPCION": "VARCHAR",
        "FCH_EMISION": "VARCHAR",
        "FCH_CADUCIDAD": "VARCHAR",
        "UBIGEO_NAC": "VARCHAR",
        "UBIGEO_DIR": "VARCHAR",
        "DIRECCION": "VARCHAR",
        "SEXO": "VARCHAR",        
        "EST_CIVIL": "VARCHAR",
        "DIG_RUC": "VARCHAR",
        "MADRE": "VARCHAR",
        "PADRE": "VARCHAR",
    }

    
    con.execute(f"""
        CREATE TABLE personas AS
        SELECT *
        FROM read_csv(
            '{TXT_PATH}',
            columns={columns},
            auto_detect=false,
            delim='{DELIM}',
            header=true,
            quote='',               -- si tu TXT tiene comillas dobles, cambia a quote='"'
            escape='',
            null_padding=true,
            ignore_errors=true,
            encoding='{ENCODING_DEFAULT}'
        );
    """)

    
    con.execute("""
        CREATE VIEW personas_v AS
        SELECT
            DNI, AP_PAT, AP_MAT, NOMBRES, FECHA_NAC, FCH_INSCRIPCION,
            FCH_EMISION, FCH_CADUCIDAD, UBIGEO_NAC, UBIGEO_DIR, DIRECCION,
            CASE TRIM(CAST(SEXO AS VARCHAR))
              WHEN '1' THEN 'MASCULINO'
              WHEN '2' THEN 'FEMENINO'
              ELSE 'DESCONOCIDO'
            END AS SEXO,
            COALESCE(NULLIF(UPPER(TRIM(EST_CIVIL)), ''), 'SOLTERO') AS EST_CIVIL,
            DIG_RUC, MADRE, PADRE
        FROM personas;
    """)

    if txt_mtime is not None:
        con.execute("INSERT OR REPLACE INTO meta VALUES ('txt_mtime', ?)", [txt_mtime])

    con.close()

def build_db_if_needed():
    # Primero intentar descargar los datos si no existen
    if not os.path.exists(TXT_PATH):
        if not download_reniec_data():
            print("No se pudieron descargar los datos, continuando sin datos")
            return
    
    txt_mtime = int(os.path.getmtime(TXT_PATH)) if os.path.exists(TXT_PATH) else None
    if not os.path.exists(DB_PATH):
        _build_db(txt_mtime); return
    con = duckdb.connect(DB_PATH)
    con.execute("CREATE TABLE IF NOT EXISTS meta(k TEXT PRIMARY KEY, v BIGINT)")
    row = con.execute("SELECT v FROM meta WHERE k='txt_mtime'").fetchone()
    current = row[0] if row else None
    if current != txt_mtime:
        _build_db(txt_mtime, con)
    con.close()


@app.on_event("startup")
def on_startup():
    global STARTUP_ERROR
    try:
        print("Iniciando procesamiento de base de datos...")
        print("Esto puede tomar varios minutos con un archivo de 5.8GB")
        build_db_if_needed()
        STARTUP_ERROR = None
        print("Base de datos procesada exitosamente")
    except Exception as e:
        STARTUP_ERROR = str(e)
        print(f"Error durante el startup: {e}")


def _ensure_ready():
    global STARTUP_ERROR
    if STARTUP_ERROR is None:
        return
    try:
        build_db_if_needed()
        STARTUP_ERROR = None
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"DB no lista: {e}")

def _fetch_all_as_dicts(con: duckdb.DuckDBPyConnection, sql: str, params: List[Any]) -> List[Dict[str, Any]]:
    cur = con.execute(sql, params)
    cols = [d[0] for d in cur.description]
    rows = cur.fetchall()
    return [dict(zip(cols, r)) for r in rows]

def _fetch_one_as_dict(con: duckdb.DuckDBPyConnection, sql: str, params: List[Any]) -> Optional[Dict[str, Any]]:
    cur = con.execute(sql, params)
    cols = [d[0] for d in cur.description]
    row = cur.fetchone()
    return dict(zip(cols, row)) if row else None

def _one(dni: str) -> Optional[Dict[str, Any]]:
    con = duckdb.connect(DB_PATH, read_only=True)
    obj = _fetch_one_as_dict(con, """
        SELECT *
        FROM personas_v
        WHERE TRIM(CAST(DNI AS VARCHAR)) = ?
        LIMIT 1
    """, [dni])
    con.close()
    return obj

def _list(ap_pat: Optional[str], ap_mat: Optional[str], nombres: Optional[str],
          ubigeo_dir: Optional[str], limit: int, offset: int) -> List[Dict[str, Any]]:
    con = duckdb.connect(DB_PATH, read_only=True)
    sql = "SELECT * FROM personas_v WHERE 1=1"
    params: List[Any] = []
    if ap_pat:
        sql += " AND LOWER(CAST(AP_PAT AS VARCHAR)) LIKE ?"; params.append(f"%{ap_pat.lower()}%")
    if ap_mat:
        sql += " AND LOWER(CAST(AP_MAT AS VARCHAR)) LIKE ?"; params.append(f"%{ap_mat.lower()}%")
    if nombres:
        sql += " AND LOWER(CAST(NOMBRES AS VARCHAR)) LIKE ?"; params.append(f"%{nombres.lower()}%")
    if ubigeo_dir:
        sql += " AND TRIM(CAST(UBIGEO_DIR AS VARCHAR)) = ?"; params.append(ubigeo_dir)
    sql += " ORDER BY DNI LIMIT ? OFFSET ?"; params += [limit, offset]
    items = _fetch_all_as_dicts(con, sql, params)
    con.close()
    return items


def _search_by_nombres(nombres_q: str, limit: int, offset: int) -> List[Dict[str, Any]]:
    """
    Busca cada palabra en NOMBRES, AP_PAT o AP_MAT (todas deben aparecer).
    """
    con = duckdb.connect(DB_PATH, read_only=True)
    terms = [t.strip().lower() for t in nombres_q.split() if t.strip()]
    if not terms:
        con.close()
        return []

    sql = "SELECT * FROM personas_v WHERE 1=1"
    params: List[Any] = []
    for t in terms:
        like = f"%{t}%"
        sql += " AND (LOWER(CAST(NOMBRES AS VARCHAR)) LIKE ? OR LOWER(CAST(AP_PAT AS VARCHAR)) LIKE ? OR LOWER(CAST(AP_MAT AS VARCHAR)) LIKE ?)"
        params.extend([like, like, like])

    sql += " ORDER BY DNI LIMIT ? OFFSET ?"
    params += [limit, offset]

    items = _fetch_all_as_dicts(con, sql, params)
    con.close()
    return items


@app.get("/health")
def health():
    info: Dict[str, Any] = {
        "status": "ok" if STARTUP_ERROR is None else "error",
        "startup_error": STARTUP_ERROR,
        "txt_exists": os.path.exists(TXT_PATH),
        "txt_path": TXT_PATH,
        "db_exists": os.path.exists(DB_PATH),
        "processing_status": "processing_database" if STARTUP_ERROR is None and not os.path.exists(DB_PATH) else "ready"
    }
    try:
        if os.path.exists(TXT_PATH):
            txt_size = os.path.getsize(TXT_PATH)
            info["txt_size_bytes"] = txt_size
            info["txt_size_gb"] = txt_size / (1024**3)

        if os.path.exists(DB_PATH):
            con = duckdb.connect(DB_PATH, read_only=True)
            try:
                info["rows_in_db"] = con.execute("SELECT COUNT(*) FROM personas").fetchone()[0]
                # Verificar si la tabla tiene datos
                sample = con.execute("SELECT * FROM personas LIMIT 1").fetchone()
                info["has_data"] = sample is not None
            except Exception as e:
                info["rows_in_db"] = None
                info["db_error"] = str(e)
            con.close()
    except Exception as e:
        info["health_error"] = str(e)
    return info

@app.post("/rebuild")
def rebuild():
    """Reconstruye manualmente cuando corrijas TXT/cabeceras."""
    global STARTUP_ERROR
    try:
        build_db_if_needed()
        STARTUP_ERROR = None
        return {"status": "rebuilt"}
    except Exception as e:
        STARTUP_ERROR = str(e)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/personas/buscar")
def buscar_por_nombres(
    nombres_q: str,
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
):
    _ensure_ready()
    return _search_by_nombres(nombres_q, limit, offset)

@app.get("/personas/{dni}")
def get_persona(dni: str):
    _ensure_ready()
    obj = _one(dni)
    if not obj:
        raise HTTPException(status_code=404, detail="No encontrado")
    return obj

@app.get("/personas")
def listar_personas(
    ap_pat: Optional[str] = Query(None, description="Filtro ILIKE por apellido paterno"),
    ap_mat: Optional[str] = Query(None, description="Filtro ILIKE por apellido materno"),
    nombres: Optional[str] = Query(None, description="Filtro ILIKE por nombres"),
    ubigeo_dir: Optional[str] = Query(None, description="Filtro exacto por ubigeo dir"),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
):
    _ensure_ready()
    return _list(ap_pat, ap_mat, nombres, ubigeo_dir, limit, offset)

@app.get("/")
def root():
    return {
        "message": "API RENIEC funcionando correctamente",
        "status": "ok",
        "data_loaded": os.path.exists(DB_PATH),
        "endpoints": [
            "/personas/{dni} - Búsqueda por DNI",
            "/personas/buscar?nombres_q={nombres} - Búsqueda por nombres completos",
            "/personas?ap_pat={apellido} - Búsqueda por apellido paterno",
            "/personas?ap_mat={apellido} - Búsqueda por apellido materno",
            "/personas?nombres={nombre} - Búsqueda por nombre",
            "/health - Estado detallado de la API",
            "/status - Estado simple (siempre responde)",
            "/rebuild - Reconstruir base de datos"
        ]
    }

@app.get("/status")
def status():
    """Endpoint simple que siempre responde para verificar que la API está viva"""
    return {
        "status": "alive",
        "message": "API RENIEC está ejecutándose",
        "timestamp": "2024"
    }

@app.get("/reniec/info")
def get_info():
    """Información sobre el estado de la API RENIEC"""
    info = {
        "status": "API funcionando",
        "data_loaded": os.path.exists(DB_PATH),
        "txt_exists": os.path.exists(TXT_PATH),
        "message": "API RENIEC con DuckDB - Optimizada para archivos grandes",
        "endpoints": [
            "/personas/{dni} - Búsqueda por DNI",
            "/personas/buscar?nombres_q={nombres} - Búsqueda por nombres completos",
            "/personas?ap_pat={apellido} - Búsqueda por apellido paterno",
            "/personas?ap_mat={apellido} - Búsqueda por apellido materno",
            "/personas?nombres={nombre} - Búsqueda por nombre",
            "/health - Estado de la API",
            "/rebuild - Reconstruir base de datos"
        ]
    }
    
    if os.path.exists(DB_PATH):
        try:
            con = duckdb.connect(DB_PATH, read_only=True)
            info["total_records"] = con.execute("SELECT COUNT(*) FROM personas").fetchone()[0]
            con.close()
        except Exception as e:
            info["error"] = str(e)
    
    return info
