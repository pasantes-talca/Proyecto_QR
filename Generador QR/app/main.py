"""
TalcaQR – API principal (FastAPI)

Punto de entrada de la aplicación.
Registra los routers y sirve el frontend estático.

Arrancar con:
    python app/main.py
  o bien:
    uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
"""
import os
import sys

# ── Asegura que la RAÍZ del proyecto esté en sys.path ─────────────────────────
# Esto permite ejecutar directamente con "python app/main.py"
# además de la forma estándar con uvicorn desde la raíz.
_APP_DIR  = os.path.dirname(os.path.abspath(__file__))   # .../Generador QR/app
_ROOT_DIR = os.path.dirname(_APP_DIR)                    # .../Generador QR
if _ROOT_DIR not in sys.path:
    sys.path.insert(0, _ROOT_DIR)

# Directorio de la carpeta static (sibling de app/)
_STATIC_DIR = os.path.join(_ROOT_DIR, "static")

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from app.config import settings
from app.routers import cache, products, qr

# ──────────────────────────────────────────────
#  Instancia de la app
# ──────────────────────────────────────────────

app = FastAPI(
    title=settings.app_title,
    version=settings.app_version,
    description=(
        "API para generación de etiquetas QR en PDF. "
        "Lee productos de la base de datos PostgreSQL (produccion.productos) "
        "y gestiona el N° de serie mediante caché local."
    ),
    docs_url="/docs",
    redoc_url="/redoc",
)

# ──────────────────────────────────────────────
#  Middlewares
# ──────────────────────────────────────────────

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],       # Ajustar en producción
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["X-Last-Serie", "X-Lote", "X-Filename"],
)

# ──────────────────────────────────────────────
#  Routers
# ──────────────────────────────────────────────

app.include_router(products.router)
app.include_router(cache.router)
app.include_router(qr.router)

# ──────────────────────────────────────────────
#  Frontend estático
# ──────────────────────────────────────────────

app.mount("/static", StaticFiles(directory=_STATIC_DIR), name="static")


@app.get("/", include_in_schema=False)
def root() -> FileResponse:
    return FileResponse(os.path.join(_STATIC_DIR, "index.html"))


# ──────────────────────────────────────────────
#  Health check
# ──────────────────────────────────────────────

@app.get("/health", tags=["Sistema"])
def health() -> dict:
    return {"status": "ok", "version": settings.app_version}


# ──────────────────────────────────────────────
#  Arranque directo con: python app/main.py
# ──────────────────────────────────────────────

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        reload_dirs=[_ROOT_DIR],
    )