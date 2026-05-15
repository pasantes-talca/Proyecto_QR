"""
Configuración central de la aplicación.
Los valores pueden sobreescribirse con variables de entorno con prefijo TALCA_
o editando el archivo config.json en el directorio raíz.
"""
from __future__ import annotations

import json
import os
from pathlib import Path

from pydantic import BaseModel

# ──────────────────────────────────────────────
#  Modelos de configuración
# ──────────────────────────────────────────────

class PostgresConfig(BaseModel):
    host: str = "10.242.4.13"
    port: int = 5432
    dbname: str = "stock_copia"
    user: str = "postgres"
    password: str = "Talca2025"
    client_encoding: str = "WIN1252"
    schema_name: str = "produccion"
    table_products: str = "productos"


class AppConfig(BaseModel):
    pg: PostgresConfig = PostgresConfig()
    cache_file: str = "cache.json"
    app_title: str = "TalcaQR"
    app_version: str = "2.0.0"


# ──────────────────────────────────────────────
#  Carga de configuración
# ──────────────────────────────────────────────

CONFIG_FILE = Path(os.environ.get("TALCA_CONFIG", "config.json"))


def load_config() -> AppConfig:
    """
    Lee config.json si existe y lo fusiona sobre los valores por defecto.
    Los campos ausentes mantienen su valor por defecto.
    """
    if CONFIG_FILE.exists():
        try:
            raw = json.loads(CONFIG_FILE.read_text(encoding="utf-8"))
            return AppConfig(**raw)
        except Exception as exc:
            print(f"[WARN] No se pudo parsear {CONFIG_FILE}: {exc}. Usando valores por defecto.")
    return AppConfig()


# Instancia global — importar desde cualquier módulo
settings: AppConfig = load_config()