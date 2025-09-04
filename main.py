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
GOOGLE_DRIVE_URL = "https://reniec-data.b-cdn.net/reniec.txt"

app = FastAPI(title="API RENIEC con DuckDB")
STARTUP_ERROR: Optional[str] = None

def load_reniec_from_stream():
    """Carga los datos de RENIEC directamente desde Bunny Storage en streaming"""
    print("Conectando a Bunny Storage...")
    print(f"URL: {GOOGLE_DRIVE_URL}")

    try:
        # Conectar al stream
        response = requests.get(GOOGLE_DRIVE_URL, stream=True, timeout=60)
        response.raise_for_status()

        # Verificar que no sea HTML de error
        peek = response.raw.read(1024)
        first_line = peek.decode("utf-8", errors="ignore").split("\n")[0]
        if first_line.startswith("<!DOCTYPE") or first_line.startswith("<html"):
            print("ERROR: La URL devuelve HTML en lugar de datos TXT")
            print(f"Contenido: {first_line[:200]}...")
            return False

        # Volvemos el puntero al inicio
        response.close()
        response = requests.get(GOOGLE_DRIVE_URL, stream=True, timeout=60)

        print("Conexión exitosa a Bunny Storage")

        # Conectar DuckDB
        con = duckdb.connect(DB_PATH)

        # Definir columnas
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

        # Crear tabla
        con.execute("DROP TABLE IF EXISTS personas")
        create_table_sql = f"""
        CREATE TABLE personas (
            {', '.join([f'{col} {dtype}' for col, dtype in columns.items()])}
        )
        """
        con.execute(create_table_sql)

        print("Procesando datos en streaming...")

        # Procesar línea por línea
        batch_size = 10000
        batch_data = []
        lines_processed = 0
        buffer = ""

        for chunk in response.iter_content(chunk_size=1024*1024):  # 1 MB por chunk
            buffer += chunk.decode("utf-8", errors="ignore")
            lines = buffer.split("\n")
            buffer = lines.pop()  # Mantener última línea incompleta

            for line in lines:
                if not line.strip():
                    continue

                fields = line.split(DELIM)
                if len(fields) < len(columns):
                    continue

                row_data = tuple(fields[:len(columns)])
                batch_data.append(row_data)
                lines_processed += 1

                # Insertar lote
                if len(batch_data) >= batch_size:
                    placeholders = ", ".join(["?"] * len(columns))
                    insert_sql = f"INSERT INTO personas VALUES ({placeholders})"
                    con.executemany(insert_sql, batch_data)
                    print(f"Procesadas {lines_processed} líneas...")
                    batch_data.clear()

        # Insertar el último lote
        if batch_data:
            placeholders = ", ".join(["?"] * len(columns))
            insert_sql = f"INSERT INTO personas VALUES ({placeholders})"
            con.executemany(insert_sql, batch_data)

        print(f"Procesamiento completado. Total líneas: {lines_processed}")

        # Crear vista optimizada
        con.execute("""
        CREATE OR REPLACE VIEW personas_v AS
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

        # Contar filas cargadas
        total_rows = con.execute("SELECT COUNT(*) FROM personas").fetchone()[0]
        print(f"Base de datos creada exitosamente con {total_rows} registros")

        con.close()
        return True

    except Exception as e:
        print(f"Error al procesar datos desde stream: {e}")
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
    # Si la base de datos ya existe, verificar si tiene datos
    if os.path.exists(DB_PATH):
        try:
            con = duckdb.connect(DB_PATH, read_only=True)
            total_rows = con.execute("SELECT COUNT(*) FROM personas").fetchone()[0]
            con.close()
            if total_rows > 0:
                print(f"Base de datos ya existe con {total_rows} registros")
                return
        except Exception as e:
            print(f"Error al verificar base de datos existente: {e}")

    # Si no hay base de datos o está vacía, cargar desde stream
    print("Cargando datos desde Bunny Storage...")
    if not load_reniec_from_stream():
        print("Error al cargar datos desde stream")
        return

    print("Base de datos creada exitosamente desde stream")


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
