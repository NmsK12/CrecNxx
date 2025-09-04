# API RENIEC con DuckDB

API REST para consultar datos del RENIEC usando DuckDB como motor de base de datos.

## Características

- **Búsqueda por DNI**: Consulta rápida por número de DNI
- **Búsqueda por nombres**: Búsqueda flexible por nombres completos
- **Filtros avanzados**: Búsqueda por apellido paterno, materno, nombres
- **Optimizada para archivos grandes**: Usa DuckDB para manejar grandes volúmenes de datos
- **Descarga automática**: Descarga los datos desde Google Drive automáticamente

## Endpoints

### Información general
- `GET /` - Información general de la API
- `GET /health` - Estado de salud de la API
- `GET /reniec/info` - Información detallada del estado

### Consultas de personas
- `GET /personas/{dni}` - Buscar persona por DNI
- `GET /personas/buscar?nombres_q={nombres}` - Buscar por nombres completos
- `GET /personas?ap_pat={apellido}&ap_mat={apellido}&nombres={nombre}` - Búsqueda con filtros

### Administración
- `POST /rebuild` - Reconstruir la base de datos

## Instalación

1. Clona el repositorio:
```bash
git clone https://github.com/NmsK12/CrecNxx.git
cd CrecNxx
```

2. Instala las dependencias:
```bash
pip install -r requirements.txt
```

3. Ejecuta la aplicación:
```bash
uvicorn main:app --host 0.0.0.0 --port 8000 --reload
```

## Despliegue en Railway

Este proyecto está configurado para desplegarse fácilmente en Railway:

1. Conecta tu repositorio de GitHub a Railway
2. Railway detectará automáticamente la configuración de Python
3. La aplicación se desplegará automáticamente

### Variables de entorno (opcional)
- `PORT`: Puerto en el que corre la aplicación (por defecto: 8000)

## Uso

### Ejemplos de consultas

```bash
# Buscar por DNI
curl "http://localhost:8000/personas/12345678"

# Buscar por nombres
curl "http://localhost:8000/personas/buscar?nombres_q=Juan Perez"

# Buscar con filtros
curl "http://localhost:8000/personas?ap_pat=Perez&nombres=Juan"
```

## Documentación de la API

Cuando la aplicación esté corriendo, puedes acceder a la documentación interactiva en:
- Swagger UI: `http://localhost:8000/docs`
- ReDoc: `http://localhost:8000/redoc`

## Tecnologías utilizadas

- **FastAPI**: Framework web moderno y rápido
- **DuckDB**: Base de datos analítica embebida
- **Uvicorn**: Servidor ASGI de alto rendimiento
- **Python 3.8+**: Lenguaje de programación

## Estructura del proyecto

```
.
├── main.py              # Aplicación principal
├── requirements.txt     # Dependencias
├── README.md           # Documentación
└── .gitignore          # Archivos ignorados por Git
```

## Notas importantes

- Los datos del RENIEC se descargan automáticamente desde Google Drive
- La base de datos se crea automáticamente al iniciar la aplicación
- Los archivos `reniec.txt` y `personas.duckdb` no están incluidos en el repositorio por razones de privacidad y tamaño

## Licencia

Este proyecto es privado y confidencial.
