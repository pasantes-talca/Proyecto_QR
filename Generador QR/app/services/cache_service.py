"""
Servicio de caché local.

Guarda el último N° de serie generado por producto en un archivo JSON
(cache.json en el directorio de trabajo). No depende de la base de datos.

Estructura interna del JSON:
{
  "last_serie::12": 47,
  "last_serie::99": 100,
  ...
}
"""
from __future__ import annotations

import json
import threading
from pathlib import Path

from app.config import settings

# Lock para escrituras concurrentes (si múltiples requests llegan al mismo tiempo)
_lock = threading.Lock()


def _cache_path() -> Path:
    return Path(settings.cache_file)


def _load() -> dict:
    path = _cache_path()
    if path.exists():
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return {}
    return {}


def _save(data: dict) -> None:
    _cache_path().write_text(
        json.dumps(data, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def _key(product_id: int) -> str:
    return f"last_serie::{product_id}"


# ──────────────────────────────────────────────
#  API pública
# ──────────────────────────────────────────────

def get_serie(product_id: int) -> int:
    """Devuelve el último N° de serie generado para el producto (0 si nunca se generó)."""
    with _lock:
        data = _load()
    try:
        return int(data.get(_key(product_id), 0))
    except (TypeError, ValueError):
        return 0


def set_serie(product_id: int, value: int) -> None:
    """Persiste el último N° de serie generado para el producto."""
    with _lock:
        data = _load()
        data[_key(product_id)] = int(value)
        _save(data)


def reset_serie(product_id: int) -> None:
    """Resetea a 0 el N° de serie (próxima impresión arrancará desde 1)."""
    set_serie(product_id, 0)