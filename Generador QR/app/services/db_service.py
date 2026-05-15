"""
Servicio de base de datos PostgreSQL.

Solo lectura: únicamente se usa para obtener los productos
de la tabla produccion.productos.
"""
from __future__ import annotations

from contextlib import contextmanager
from typing import Generator

try:
    import psycopg2
    import psycopg2.extensions
    _PSYCOPG2_AVAILABLE = True
except ImportError:
    _PSYCOPG2_AVAILABLE = False

from app.config import settings


class DBUnavailableError(Exception):
    """Se lanza cuando psycopg2 no está instalado o la conexión falla."""


@contextmanager
def get_connection() -> Generator:
    """Context manager que abre y cierra la conexión a PostgreSQL."""
    if not _PSYCOPG2_AVAILABLE:
        raise DBUnavailableError(
            "psycopg2 no está instalado. "
            "Ejecutá: pip install psycopg2-binary"
        )

    cfg = settings.pg
    try:
        conn = psycopg2.connect(
            host=cfg.host,
            port=cfg.port,
            dbname=cfg.dbname,
            user=cfg.user,
            password=cfg.password,
        )
        if cfg.client_encoding:
            conn.set_client_encoding(cfg.client_encoding)
        conn.autocommit = True
        try:
            yield conn
        finally:
            conn.close()
    except psycopg2.OperationalError as exc:
        raise DBUnavailableError(f"No se pudo conectar a PostgreSQL: {exc}") from exc


def fetch_products() -> list[tuple[int, str]]:
    """
    Devuelve todos los productos de produccion.productos
    ordenados alfabéticamente por descripción.

    Returns:
        Lista de tuplas (id, descripcion).
    """
    cfg = settings.pg
    schema = cfg.schema_name
    table = cfg.table_products

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT id, descripcion
                FROM {schema}.{table}
                ORDER BY descripcion ASC;
                """
            )
            rows: list[tuple] = cur.fetchall()

    return [(int(row[0]), str(row[1])) for row in rows]