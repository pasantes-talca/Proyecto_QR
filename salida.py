import os
import re
import sys
import json
import uuid
import calendar
import smtplib
import threading
import unicodedata
import urllib.error
import urllib.request
import tkinter as tk
from datetime import datetime
from email.message import EmailMessage
from tkinter import messagebox, ttk

import ttkbootstrap as tb
from ttkbootstrap.constants import DANGER, INFO, SECONDARY, WARNING

try:
    import psycopg2
except Exception:
    psycopg2 = None

ROOT = None
STATUS_TEXT = None
USUARIO_AJUSTE_ACTUAL = None
SYNC_TIMER = None
SYNC_LOCK = threading.Lock()

APP_TITLE = "Sistema de Bajas – Talca"
EMAIL_AJUSTES_TO = "pasantes@talca.com.ar"
EMAIL_BAJAS_TO = "pasantes@talca.com.ar"

EMAIL_SMTP_HOST = "pasantes@talca.com.ar"
EMAIL_SMTP_PORT = 587
EMAIL_SMTP_USER = "pasantes@talca.com.ar"
EMAIL_SMTP_PASSWORD = os.environ.get("TALCA_EMAIL_PASSWORD", "iguc wjzp bfos nbei")
EMAIL_FROM = "pasantes@talca.com.ar"
EMAIL_USE_TLS = True

LOTE_AJUSTE_DEFAULT = "AJUSTE"
MOTIVOS = ("Venta", "Calidad", "Desarme", "Observacion")

DEFAULT_PG = {
    "host": "10.242.4.13",
    "port": 5432,
    "dbname": "stock_copia",
    "user": "postgres",
    "password": "Talca2025",
    "client_encoding": "WIN1252",
    "schema": "produccion",
    "table_products": "productos",
    "table_stock": "stock",
    "table_bajas": "bajas",
    "table_clients": "clientes",
}

DEFAULT_SHEET = {
    "webapp_url": "https://script.google.com/macros/s/AKfycbyBxCyZMyEeFi_fdsRu-SjnZHktTH4XNF3APKaJJsnqoesgTKjZ8sSqF_O6NDwXa77s9Q/exec",
    "api_key": "TALCA-QR-2026",
}

CLIENTES_INICIALES = [
    "Rojo", "Aiobak", "Escudero", "Scifo", "Abraham", "Gatica", "Mariano", "Ochoa", "Martos",
    "Garro", "Deposito San Luis", "Deposito San Juan", "Depósito Salta", "Depósito Martins", "Preventa",
]


def app_dir():
    return os.path.dirname(sys.executable) if getattr(sys, "frozen", False) else os.path.dirname(os.path.abspath(__file__))


APP_DIR = app_dir()
CONFIG_FILE = os.path.join(APP_DIR, "config.json")
LAST_USER_FILE = os.path.join(APP_DIR, "ultimo_usuario_ajuste.txt")


def load_config():
    try:
        with open(CONFIG_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def pg_config():
    data = load_config().get("pg", {})
    cfg = {**DEFAULT_PG, **{k: v for k, v in data.items() if v not in (None, "")}}
    cfg["port"] = int(cfg.get("port") or 5432)
    return cfg


def sheet_config():
    data = load_config().get("sheet", {})
    return {**DEFAULT_SHEET, **{k: v for k, v in data.items() if v not in (None, "")}}


def email_config():
    smtp_host = EMAIL_SMTP_HOST
    if "@" in smtp_host:
        smtp_host = "smtp.gmail.com"

    from_addr = EMAIL_FROM.split(",")[0].strip() if EMAIL_FROM else EMAIL_SMTP_USER

    return {
        "smtp_host": smtp_host,
        "smtp_port": int(EMAIL_SMTP_PORT),
        "smtp_user": EMAIL_SMTP_USER,
        "smtp_password": EMAIL_SMTP_PASSWORD,
        "from": from_addr,
        "use_tls": EMAIL_USE_TLS,
    }


def ident(value):
    value = str(value or "").strip()
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value):
        raise ValueError(f"Identificador SQL inválido: {value}")
    return value


def table(name):
    cfg = pg_config()
    return f"{ident(cfg['schema'])}.{ident(cfg[name])}"


def custom_table(name):
    return f"{ident(pg_config()['schema'])}.{ident(name)}"


def normalize_name(value):
    return " ".join(str(value or "").strip().split())


def normalize_key(value):
    value = unicodedata.normalize("NFKD", normalize_name(value))
    return "".join(ch for ch in value if not unicodedata.combining(ch)).casefold()


def add_months(date_value, months):
    month = date_value.month - 1 + months
    year = date_value.year + month // 12
    month = month % 12 + 1
    day = min(date_value.day, calendar.monthrange(year, month)[1])
    return date_value.replace(year=year, month=month, day=day)


def set_status(text):
    if STATUS_TEXT is None:
        return
    STATUS_TEXT.configure(state="normal")
    STATUS_TEXT.delete("1.0", "end")
    STATUS_TEXT.insert("end", str(text))
    STATUS_TEXT.see("end")
    STATUS_TEXT.configure(state="disabled")


def append_status(text):
    if STATUS_TEXT is None:
        return
    current = STATUS_TEXT.get("1.0", "end-1c").strip()
    STATUS_TEXT.configure(state="normal")
    STATUS_TEXT.insert("end", ("\n" if current else "") + str(text))
    STATUS_TEXT.see("end")
    STATUS_TEXT.configure(state="disabled")


def run_thread(fn):
    threading.Thread(target=fn, daemon=True).start()


def pg_connect():
    if psycopg2 is None:
        raise RuntimeError("Falta psycopg2. Instalá con: pip install psycopg2-binary")
    cfg = pg_config()
    conn = psycopg2.connect(
        host=cfg["host"], port=cfg["port"], dbname=cfg["dbname"], user=cfg["user"], password=cfg["password"]
    )
    conn.autocommit = True
    if cfg.get("client_encoding"):
        conn.set_client_encoding(cfg["client_encoding"])
    return conn


def columns(conn, table_name):
    cfg = pg_config()
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            """,
            (cfg["schema"], cfg[table_name] if table_name.startswith("table_") else table_name),
        )
        return {str(r[0]).lower() for r in cur.fetchall()}


def init_tables(conn):
    cfg = pg_config()
    schema = ident(cfg["schema"])
    bajas = table("table_bajas")
    clients = table("table_clients")
    users = custom_table("usuarios_ajuste")
    with conn.cursor() as cur:
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {bajas} (
                id SERIAL PRIMARY KEY,
                id_producto INTEGER,
                stock_lote TEXT,
                fecha_hora TIMESTAMP NOT NULL DEFAULT NOW(),
                cantidad INTEGER NOT NULL DEFAULT 1
            )
        """)
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {clients} (
                id SERIAL PRIMARY KEY,
                nombre TEXT NOT NULL,
                nombre_normalizado TEXT NOT NULL UNIQUE,
                activo BOOLEAN NOT NULL DEFAULT TRUE,
                fecha_alta TIMESTAMP NOT NULL DEFAULT NOW()
            )
        """)
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {users} (
                id SERIAL PRIMARY KEY,
                usuario TEXT NOT NULL UNIQUE,
                contrasena TEXT NOT NULL,
                activo BOOLEAN NOT NULL DEFAULT TRUE,
                fecha_alta TIMESTAMP NOT NULL DEFAULT NOW()
            )
        """)
        for col, definition in {
            "motivo": "TEXT NOT NULL DEFAULT ''",
            "observaciones": "TEXT NOT NULL DEFAULT ''",
            "tipo_unidad": "TEXT",
            "cliente": "TEXT",
            "id_cliente": "INTEGER",
            "nro_serie": "INTEGER",
        }.items():
            cur.execute(f"""
                DO $$ BEGIN
                    IF NOT EXISTS (
                        SELECT 1 FROM information_schema.columns
                        WHERE table_schema = '{schema}' AND table_name = '{ident(cfg['table_bajas'])}' AND column_name = '{col}'
                    ) THEN ALTER TABLE {bajas} ADD COLUMN {col} {definition};
                    END IF;
                END $$;
            """)
        cur.execute(f"""
            DO $$ BEGIN
                IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_bajas_clientes' AND connamespace = '{schema}'::regnamespace) THEN
                    ALTER TABLE {bajas} ADD CONSTRAINT fk_bajas_clientes FOREIGN KEY (id_cliente) REFERENCES {clients}(id);
                END IF;
            END $$;
        """)
        cur.execute(f"""
            INSERT INTO {users} (usuario, contrasena, activo)
            VALUES ('Ariel Garro', 'Talca2026**', TRUE), ('Carlos Bagnardi', 'Talca2026*', TRUE)
            ON CONFLICT (usuario) DO UPDATE SET contrasena = EXCLUDED.contrasena, activo = TRUE
        """)
        for nombre in CLIENTES_INICIALES:
            cur.execute(
                f"""
                INSERT INTO {clients} (nombre, nombre_normalizado, activo, fecha_alta)
                VALUES (%s, %s, TRUE, NOW())
                ON CONFLICT (nombre_normalizado) DO NOTHING
                """,
                (normalize_name(nombre), normalize_key(nombre)),
            )


def load_last_user():
    try:
        with open(LAST_USER_FILE, "r", encoding="utf-8") as f:
            return f.read().strip()
    except Exception:
        return ""


def save_last_user(user):
    try:
        if normalize_name(user):
            with open(LAST_USER_FILE, "w", encoding="utf-8") as f:
                f.write(normalize_name(user))
    except Exception:
        pass


def authenticate_adjust_user(conn, user, password):
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT usuario
            FROM {custom_table('usuarios_ajuste')}
            WHERE LOWER(BTRIM(usuario)) = LOWER(BTRIM(%s)) AND contrasena = %s AND activo = TRUE
            LIMIT 1
            """,
            (normalize_name(user), str(password or "")),
        )
        row = cur.fetchone()
    return normalize_name(row[0]) if row else None


def get_clients(conn):
    with conn.cursor() as cur:
        cur.execute(f"SELECT nombre FROM {table('table_clients')} WHERE activo = TRUE ORDER BY nombre_normalizado, nombre")
        return [normalize_name(r[0]) for r in cur.fetchall()]


def ensure_client(conn, name):
    name = normalize_name(name)
    if not name:
        raise ValueError("Cliente vacío.")
    key = normalize_key(name)
    with conn.cursor() as cur:
        cur.execute(f"SELECT id, nombre, activo FROM {table('table_clients')} WHERE nombre_normalizado = %s LIMIT 1", (key,))
        row = cur.fetchone()
        if row:
            if not bool(row[2]):
                cur.execute(f"UPDATE {table('table_clients')} SET activo = TRUE WHERE id = %s", (row[0],))
            return int(row[0]), normalize_name(row[1]), False
        cur.execute(
            f"""
            INSERT INTO {table('table_clients')} (nombre, nombre_normalizado, activo, fecha_alta)
            VALUES (%s, %s, TRUE, NOW()) RETURNING id, nombre
            """,
            (name, key),
        )
        row = cur.fetchone()
    return int(row[0]), normalize_name(row[1]), True


def unit_sql(alias=""):
    col = f"{alias}.tipo_unidad" if alias else "tipo_unidad"
    return f"UPPER(BTRIM(COALESCE({col}, '')))"


def product_options_with_stock(conn):
    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT p.id, p.descripcion
            FROM {table('table_products')} p
            WHERE EXISTS (
                SELECT 1 FROM {table('table_stock')} s
                WHERE s.id_producto = p.id
                  AND ({unit_sql('s')} IN ('PALLET', 'PALLETS') OR ({unit_sql('s')} = 'PACKS' AND COALESCE(s.packs, 0) > 0))
            )
            ORDER BY p.id
        """)
        return [(int(r[0]), normalize_name(r[1]) or "Sin descripción") for r in cur.fetchall()]


def product_stock_summary(conn, only_with_stock=True):
    having = """
        HAVING COALESCE(SUM(CASE WHEN UPPER(BTRIM(COALESCE(s.tipo_unidad, ''))) IN ('PALLET','PALLETS') THEN 1 ELSE 0 END), 0) > 0
            OR COALESCE(SUM(CASE WHEN UPPER(BTRIM(COALESCE(s.tipo_unidad, ''))) = 'PACKS' THEN COALESCE(s.packs, 0) ELSE 0 END), 0) > 0
    """ if only_with_stock else ""
    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT p.id, p.descripcion,
                   COALESCE(SUM(CASE WHEN {unit_sql('s')} IN ('PALLET','PALLETS') THEN 1 ELSE 0 END), 0) AS pallets,
                   COALESCE(SUM(CASE WHEN {unit_sql('s')} = 'PACKS' THEN COALESCE(s.packs, 0) ELSE 0 END), 0) AS packs
            FROM {table('table_products')} p
            LEFT JOIN {table('table_stock')} s ON s.id_producto = p.id
            GROUP BY p.id, p.descripcion
            {having}
            ORDER BY p.id
        """)
        return [(int(r[0]), normalize_name(r[1]) or "Sin descripción", int(r[2] or 0), int(r[3] or 0)) for r in cur.fetchall()]


def product_stock(conn, product_id):
    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT p.descripcion,
                   COALESCE(SUM(CASE WHEN {unit_sql('s')} IN ('PALLET','PALLETS') THEN 1 ELSE 0 END), 0),
                   COALESCE(SUM(CASE WHEN {unit_sql('s')} = 'PACKS' THEN COALESCE(s.packs, 0) ELSE 0 END), 0)
            FROM {table('table_products')} p
            LEFT JOIN {table('table_stock')} s ON s.id_producto = p.id
            WHERE p.id = %s
            GROUP BY p.descripcion
        """, (int(product_id),))
        row = cur.fetchone()
    return (normalize_name(row[0]) or "Sin descripción", int(row[1] or 0), int(row[2] or 0)) if row else ("Sin descripción", 0, 0)


def product_lotes(conn, product_id):
    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT lote::text, MIN(nro_serie) AS orden
            FROM {table('table_stock')}
            WHERE id_producto = %s
              AND BTRIM(COALESCE(lote::text, '')) <> ''
              AND UPPER(BTRIM(COALESCE(lote::text, ''))) <> 'AJUSTE'
              AND ({unit_sql()} IN ('PALLET','PALLETS') OR ({unit_sql()} = 'PACKS' AND COALESCE(packs, 0) > 0))
            GROUP BY lote::text
            ORDER BY orden ASC NULLS LAST, lote::text ASC
        """, (int(product_id),))
        return [str(r[0]).strip() for r in cur.fetchall()]


def lote_stock(conn, product_id, lote):
    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT COALESCE(SUM(CASE WHEN {unit_sql()} IN ('PALLET','PALLETS') THEN 1 ELSE 0 END), 0),
                   COALESCE(SUM(CASE WHEN {unit_sql()} = 'PACKS' THEN COALESCE(packs, 0) ELSE 0 END), 0)
            FROM {table('table_stock')}
            WHERE id_producto = %s AND BTRIM(COALESCE(lote::text, '')) = BTRIM(%s)
        """, (int(product_id), str(lote)))
        row = cur.fetchone()
    return int(row[0] or 0), int(row[1] or 0)


def available_by_lote_unit(conn, product_id, lote, unit):
    unit = str(unit or "").upper().strip()
    if unit in ("PALLET", "PALLETS"):
        expr = f"COUNT(*) FROM {table('table_stock')} WHERE id_producto = %s AND BTRIM(COALESCE(lote::text, '')) = BTRIM(%s) AND {unit_sql()} IN ('PALLET','PALLETS')"
    elif unit == "PACKS":
        expr = f"COALESCE(SUM(COALESCE(packs, 0)), 0) FROM {table('table_stock')} WHERE id_producto = %s AND BTRIM(COALESCE(lote::text, '')) = BTRIM(%s) AND {unit_sql()} = 'PACKS' AND COALESCE(packs, 0) > 0"
    else:
        return 0
    with conn.cursor() as cur:
        cur.execute(f"SELECT {expr}", (int(product_id), str(lote)))
        return int(cur.fetchone()[0] or 0)


def insert_baja(conn, product_id, lote, nro_serie, motivo, observaciones, unit, cliente=None, client_id=None, cantidad=1):
    with conn.cursor() as cur:
        cur.execute(
            f"""
            INSERT INTO {table('table_bajas')}
            (id_producto, stock_lote, nro_serie, fecha_hora, cantidad, motivo, observaciones, tipo_unidad, cliente, id_cliente)
            VALUES (%s, %s, %s, NOW(), %s, %s, %s, %s, %s, %s)
            RETURNING id
            """,
            (
                int(product_id), str(lote), int(nro_serie) if nro_serie is not None else None, int(cantidad),
                str(motivo or ""), str(observaciones or ""), str(unit or "").upper().strip(),
                normalize_name(cliente) or None, int(client_id) if client_id is not None else None,
            ),
        )
        return int(cur.fetchone()[0])


def selected_stock_for_baja(conn, product_id, lote, unit, cantidad):
    unit = str(unit).upper().strip()
    cantidad = int(cantidad)
    affected = []
    with conn.cursor() as cur:
        if unit == "PALLET":
            cur.execute(f"""
                SELECT ctid::text, nro_serie
                FROM {table('table_stock')}
                WHERE id_producto = %s
                  AND BTRIM(COALESCE(lote::text, '')) = BTRIM(%s)
                  AND {unit_sql()} IN ('PALLET','PALLETS')
                ORDER BY nro_serie ASC
                LIMIT %s
                FOR UPDATE
            """, (int(product_id), str(lote), cantidad))
            rows = cur.fetchall()
            if len(rows) < cantidad:
                raise ValueError(f"No hay pallets suficientes en ese lote. Disponibles: {len(rows)}")
            for ctid, serie in rows:
                cur.execute(f"DELETE FROM {table('table_stock')} WHERE ctid = %s::tid", (ctid,))
                if cur.rowcount != 1:
                    raise ValueError(f"No se pudo eliminar la serie {serie}.")
                affected.append({"nro_serie": int(serie) if serie is not None else None, "cantidad": 1})
            return affected

        if unit == "PACKS":
            cur.execute(f"""
                SELECT ctid::text, nro_serie, COALESCE(packs, 0)
                FROM {table('table_stock')}
                WHERE id_producto = %s
                  AND BTRIM(COALESCE(lote::text, '')) = BTRIM(%s)
                  AND {unit_sql()} = 'PACKS'
                  AND COALESCE(packs, 0) > 0
                ORDER BY nro_serie ASC
                FOR UPDATE
            """, (int(product_id), str(lote)))
            rows = cur.fetchall()
            available = sum(int(r[2] or 0) for r in rows)
            if available < cantidad:
                raise ValueError(f"No hay packs suficientes en ese lote. Disponibles: {available}")
            remaining = cantidad
            for ctid, serie, packs in rows:
                if remaining <= 0:
                    break
                take = min(int(packs or 0), remaining)
                if take == int(packs or 0):
                    cur.execute(f"DELETE FROM {table('table_stock')} WHERE ctid = %s::tid", (ctid,))
                else:
                    cur.execute(f"UPDATE {table('table_stock')} SET packs = packs - %s WHERE ctid = %s::tid", (take, ctid))
                if cur.rowcount != 1:
                    raise ValueError(f"No se pudo actualizar la serie {serie}.")
                affected.extend({"nro_serie": int(serie) if serie is not None else None, "cantidad": 1} for _ in range(take))
                remaining -= take
            return affected

    raise ValueError("Tipo de unidad inválido.")


def selected_stock_general(conn, product_id, unit, cantidad):
    unit = str(unit).upper().strip()
    cantidad = int(cantidad)
    affected = []
    by_lote = {}
    with conn.cursor() as cur:
        if unit == "PALLET":
            cur.execute(f"""
                SELECT ctid::text, nro_serie, COALESCE(lote::text, 'SIN LOTE')
                FROM {table('table_stock')}
                WHERE id_producto = %s AND {unit_sql()} IN ('PALLET','PALLETS')
                ORDER BY nro_serie ASC
                LIMIT %s
                FOR UPDATE
            """, (int(product_id), cantidad))
            rows = cur.fetchall()
            if len(rows) < cantidad:
                raise ValueError(f"No hay pallets suficientes para ajustar. Disponibles: {len(rows)}")
            for ctid, serie, lote in rows:
                cur.execute(f"DELETE FROM {table('table_stock')} WHERE ctid = %s::tid", (ctid,))
                if cur.rowcount != 1:
                    raise ValueError(f"No se pudo eliminar la serie {serie}.")
                affected.append({"lote": normalize_name(lote) or "SIN LOTE", "nro_serie": int(serie) if serie is not None else None, "cantidad": 1})

        elif unit == "PACKS":
            cur.execute(f"""
                SELECT ctid::text, nro_serie, COALESCE(lote::text, 'SIN LOTE'), COALESCE(packs, 0)
                FROM {table('table_stock')}
                WHERE id_producto = %s AND {unit_sql()} = 'PACKS' AND COALESCE(packs, 0) > 0
                ORDER BY nro_serie ASC
                FOR UPDATE
            """, (int(product_id),))
            rows = cur.fetchall()
            available = sum(int(r[3] or 0) for r in rows)
            if available < cantidad:
                raise ValueError(f"No hay packs suficientes para ajustar. Disponibles: {available}")
            remaining = cantidad
            for ctid, serie, lote, packs in rows:
                if remaining <= 0:
                    break
                take = min(int(packs or 0), remaining)
                if take == int(packs or 0):
                    cur.execute(f"DELETE FROM {table('table_stock')} WHERE ctid = %s::tid", (ctid,))
                else:
                    cur.execute(f"UPDATE {table('table_stock')} SET packs = packs - %s WHERE ctid = %s::tid", (take, ctid))
                if cur.rowcount != 1:
                    raise ValueError(f"No se pudo actualizar la serie {serie}.")
                affected.extend({"lote": normalize_name(lote) or "SIN LOTE", "nro_serie": int(serie) if serie is not None else None, "cantidad": 1} for _ in range(take))
                remaining -= take
        else:
            raise ValueError("Tipo de unidad inválido.")

    for row in affected:
        by_lote[row["lote"]] = by_lote.get(row["lote"], 0) + row["cantidad"]
    return affected, by_lote


def baja_manual(conn, product_id, lote, unit, cantidad, motivo, observaciones="", cliente=""):
    product_id = int(product_id)
    lote = normalize_name(lote)
    unit = "PALLET" if str(unit).lower().startswith("pallet") else "PACKS"
    cantidad = int(cantidad)
    motivo = normalize_name(motivo)
    observaciones = str(observaciones or "").strip()
    cliente = normalize_name(cliente)

    if not lote:
        raise ValueError("Seleccioná un lote.")
    if cantidad <= 0:
        raise ValueError("La cantidad debe ser mayor que 0.")
    if motivo == "Venta" and not cliente:
        raise ValueError("Para una baja por Venta tenés que seleccionar o escribir un cliente.")
    if motivo != "Venta":
        cliente = ""

    prev_autocommit = conn.autocommit
    try:
        conn.autocommit = False
        client_id = None
        cliente_final = None
        cliente_creado = False
        if motivo == "Venta":
            client_id, cliente_final, cliente_creado = ensure_client(conn, cliente)
        available = available_by_lote_unit(conn, product_id, lote, unit)
        if available < cantidad:
            raise ValueError(f"No hay stock suficiente. Disponible para {unit} en este lote: {available}.")
        affected = selected_stock_for_baja(conn, product_id, lote, unit, cantidad)
        baja_ids = [
            insert_baja(conn, product_id, lote, item["nro_serie"], motivo, observaciones, unit, cliente_final, client_id, 1)
            for item in affected
        ]
        desc, pallets, packs = product_stock(conn, product_id)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.autocommit = prev_autocommit

    return {
        "bajas_ids": baja_ids,
        "id_producto": product_id,
        "descripcion": desc,
        "lote": lote,
        "tipo_unidad": unit,
        "cantidad": cantidad,
        "motivo": motivo,
        "observaciones": observaciones,
        "cliente": cliente_final,
        "cliente_creado": cliente_creado,
        "series_afectadas": [item["nro_serie"] for item in affected],
        "stock_restante_pallets": pallets,
        "stock_restante_packs": packs,
    }


def next_series(conn, product_id):
    with conn.cursor() as cur:
        cur.execute(f"SELECT COALESCE(MAX(nro_serie), 0) FROM {table('table_stock')} WHERE id_producto = %s", (int(product_id),))
        return int(cur.fetchone()[0] or 0) + 1


def adjustment_lote_dates():
    today = datetime.now().date()
    return today.strftime("%d%m%y"), today.isoformat(), add_months(today, 6).isoformat()


def insert_stock_adjustment(conn, product_id, unit, cantidad):
    cantidad = int(cantidad)
    if cantidad <= 0:
        return []
    unit = str(unit).upper().strip()
    lote, creacion, vencimiento = adjustment_lote_dates()
    stock_cols = columns(conn, "table_stock")
    series = []
    start = next_series(conn, product_id)

    def insert_one(cur, serie, packs):
        data = {"id_producto": int(product_id), "lote": lote, "tipo_unidad": unit, "nro_serie": int(serie), "packs": int(packs)}
        if "creacion" in stock_cols:
            data["creacion"] = creacion
        elif "fecha_creacion" in stock_cols:
            data["fecha_creacion"] = creacion
        if "vencimiento" in stock_cols:
            data["vencimiento"] = vencimiento
        elif "fecha_vencimiento" in stock_cols:
            data["fecha_vencimiento"] = vencimiento
        if "fecha_hora" in stock_cols:
            data["fecha_hora"] = datetime.now()
        cols = ", ".join(data.keys())
        marks = ", ".join(["%s"] * len(data))
        cur.execute(f"INSERT INTO {table('table_stock')} ({cols}) VALUES ({marks})", list(data.values()))

    with conn.cursor() as cur:
        if unit == "PALLET":
            for i in range(cantidad):
                serie = start + i
                insert_one(cur, serie, 0)
                series.append(serie)
        elif unit == "PACKS":
            insert_one(cur, start, cantidad)
            series.append(start)
        else:
            raise ValueError("Tipo de unidad inválido para alta.")
    return series


def format_diff(pallets, packs):
    parts = []
    if pallets:
        parts.append(f"Pallets {pallets:+d}")
    if packs:
        parts.append(f"Packs {packs:+d}")
    return " | ".join(parts) if parts else "Sin cambios"


def apply_adjustment(conn, change, user):
    product_id = int(change["id_producto"])
    new_pallets = int(change["nuevo_pallets"])
    new_packs = int(change["nuevo_packs"])
    if new_pallets < 0 or new_packs < 0:
        raise ValueError("Los valores de ajuste no pueden ser negativos.")

    desc, old_pallets, old_packs = product_stock(conn, product_id)
    diff_p = new_pallets - old_pallets
    diff_pk = new_packs - old_packs
    baja_ids = []
    fecha = datetime.now().strftime("%d/%m/%Y")
    motivo = f"Baja por ajuste de stock {fecha} por {normalize_name(user) or 'usuario no identificado'}"

    if diff_p > 0:
        insert_stock_adjustment(conn, product_id, "PALLET", diff_p)
    elif diff_p < 0:
        affected, _ = selected_stock_general(conn, product_id, "PALLET", abs(diff_p))
        baja_ids.extend(insert_baja(conn, product_id, item["lote"], item["nro_serie"], motivo, f"Ajuste administrativo. Stock anterior: {old_pallets}. Stock nuevo: {new_pallets}.", "PALLET") for item in affected)

    if diff_pk > 0:
        insert_stock_adjustment(conn, product_id, "PACKS", diff_pk)
    elif diff_pk < 0:
        affected, _ = selected_stock_general(conn, product_id, "PACKS", abs(diff_pk))
        baja_ids.extend(insert_baja(conn, product_id, item["lote"], item["nro_serie"], motivo, f"Ajuste administrativo. Stock anterior: {old_packs}. Stock nuevo: {new_packs}.", "PACKS") for item in affected)

    final_desc, final_pallets, final_packs = product_stock(conn, product_id)
    return {
        "id_producto": product_id,
        "descripcion": final_desc or desc,
        "pallets_antes": old_pallets,
        "packs_antes": old_packs,
        "pallets_despues": final_pallets,
        "packs_despues": final_packs,
        "diff_pallets": final_pallets - old_pallets,
        "diff_packs": final_packs - old_packs,
        "bajas_ids": baja_ids,
        "usuario_ajuste": normalize_name(user) or "No identificado",
    }


def apply_adjustments(conn, changes, user):
    prev_autocommit = conn.autocommit
    results = []
    try:
        conn.autocommit = False
        for change in changes:
            result = apply_adjustment(conn, change, user)
            if result["diff_pallets"] or result["diff_packs"]:
                results.append(result)
        conn.commit()
        return results
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.autocommit = prev_autocommit


def format_ids(ids, limit=20):
    ids = list(ids or [])
    if not ids:
        return "No corresponde"
    if len(ids) <= limit:
        return ", ".join(str(x) for x in ids)
    return ", ".join(str(x) for x in ids[:limit]) + f", ... ({len(ids)} registros en total)"


def send_email(subject, body, to_addr):
    cfg = email_config()
    if not cfg["smtp_password"]:
        raise ValueError("Falta configurar EMAIL_SMTP_PASSWORD en el archivo salida.py.")
    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = cfg["from"]
    msg["To"] = to_addr
    msg.set_content(body)
    with smtplib.SMTP(cfg["smtp_host"], cfg["smtp_port"], timeout=30) as smtp:
        if cfg["use_tls"]:
            smtp.starttls()
        smtp.login(cfg["smtp_user"], cfg["smtp_password"])
        smtp.send_message(msg)


def baja_email_body(data):
    series = [str(x) for x in data.get("series_afectadas", []) if x is not None]
    if len(series) > 30:
        series_text = ", ".join(series[:30]) + f", ... ({len(series)} series en total)"
    else:
        series_text = ", ".join(series) if series else "Sin detalle"
    lines = [
        "Se registró una baja desde el sistema de stock de Talca.",
        "",
        f"Fecha y hora: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}",
        f"IDs de bajas generadas: {format_ids(data.get('bajas_ids'))}",
        f"Producto: [{data.get('id_producto')}] {data.get('descripcion')}",
        f"Lote: {data.get('lote')}",
        f"Tipo de unidad: {data.get('tipo_unidad')}",
        f"Cantidad dada de baja: {data.get('cantidad')}",
        f"Motivo: {data.get('motivo')}",
        f"Cliente: {data.get('cliente') or 'No aplica'}",
        f"Observaciones: {data.get('observaciones') or ''}",
        f"Números de serie afectados: {series_text}",
        "",
        "Stock restante luego de la baja:",
        f"Pallets: {data.get('stock_restante_pallets')}",
        f"Packs: {data.get('stock_restante_packs')}",
    ]
    return "\n".join(lines)


def send_baja_email_async(data):
    def worker():
        try:
            subject = f"Baja de stock Talca - Producto {data.get('id_producto')} - {data.get('motivo')} - {datetime.now().strftime('%d/%m/%Y %H:%M')}"
            send_email(subject, baja_email_body(data), EMAIL_BAJAS_TO)
            msg = f"📧 Mail de baja enviado correctamente a {EMAIL_BAJAS_TO}."
        except Exception as e:
            msg = f"⚠️ No se pudo enviar el mail de baja: {e}"
        if ROOT is not None:
            ROOT.after(0, lambda: append_status(msg))
    run_thread(worker)


def adjustment_email_body(changes):
    user = changes[0].get("usuario_ajuste", "No identificado") if changes else "No identificado"
    lines = [
        "Se realizaron ajustes de stock desde el sistema de Talca.",
        "",
        f"Fecha y hora: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}",
        f"Usuario que realizó el ajuste: {user}",
        f"Cantidad de productos modificados: {len(changes)}",
        "",
    ]
    for c in changes:
        lines.extend([
            f"Producto: [{c['id_producto']}] {c['descripcion']}",
            f"Stock anterior: {c['pallets_antes']} pallets / {c['packs_antes']} packs",
            f"Stock nuevo: {c['pallets_despues']} pallets / {c['packs_despues']} packs",
            f"Diferencia: {format_diff(c['diff_pallets'], c['diff_packs'])}",
            f"IDs de bajas generadas: {format_ids(c.get('bajas_ids'))}",
            "-" * 60,
        ])
    return "\n".join(lines)


def send_adjustment_email_async(changes):
    def worker():
        try:
            user = changes[0].get("usuario_ajuste", "No identificado") if changes else "No identificado"
            subject = f"Ajustes de stock Talca - {user} - {datetime.now().strftime('%d/%m/%Y %H:%M')}"
            send_email(subject, adjustment_email_body(changes), EMAIL_AJUSTES_TO)
            msg = f"📧 Mail de ajustes enviado correctamente a {EMAIL_AJUSTES_TO}."
        except Exception as e:
            msg = f"⚠️ No se pudo enviar el mail de ajustes: {e}"
        if ROOT is not None:
            ROOT.after(0, lambda: append_status(msg))
    run_thread(worker)


def post_json(payload, timeout=60):
    cfg = sheet_config()
    req = urllib.request.Request(
        cfg["webapp_url"],
        data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            txt = resp.read().decode("utf-8", errors="ignore")
            try:
                return json.loads(txt)
            except Exception:
                return {"ok": False, "raw": txt}
    except urllib.error.HTTPError as e:
        return {"ok": False, "http_status": e.code, "error": str(e), "raw": e.read().decode("utf-8", errors="ignore")}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def sheet_rows(conn):
    return [
        {"codigo": pid, "id_producto": pid, "descripcion": desc, "stock_pallets": pallets, "stock_packs": packs, "pallet": pallets, "bulto": packs}
        for pid, desc, pallets, packs in product_stock_summary(conn, only_with_stock=False)
    ]


def send_sheet_snapshot(rows, chunk_size=500):
    cfg = sheet_config()
    rows = rows or []
    snapshot_id = str(uuid.uuid4())
    if not rows:
        payload = {"api_key": cfg["api_key"], "action": "bulk_snapshot_pp", "type": "bulk_snapshot_pp", "snapshot_id": snapshot_id, "block_index": 0, "is_first_block": True, "is_last_block": True, "rows": []}
        return post_json(payload)
    for start in range(0, len(rows), chunk_size):
        payload = {
            "api_key": cfg["api_key"], "action": "bulk_snapshot_pp", "type": "bulk_snapshot_pp", "snapshot_id": snapshot_id,
            "block_index": start // chunk_size, "is_first_block": start == 0, "is_last_block": start + chunk_size >= len(rows),
            "rows": rows[start:start + chunk_size],
        }
        res = post_json(payload, timeout=90)
        if not (isinstance(res, dict) and res.get("ok") is True):
            return res
    return {"ok": True, "snapshot_id": snapshot_id}


def sync_sheet_async(delay=1.0):
    global SYNC_TIMER

    def worker():
        conn = None
        try:
            conn = pg_connect()
            res = send_sheet_snapshot(sheet_rows(conn))
            msg = "✅ Google Sheet sincronizado correctamente." if isinstance(res, dict) and res.get("ok") else f"⚠️ Google Sheet no se sincronizó correctamente: {res}"
        except Exception as e:
            msg = f"⚠️ Error sincronizando Google Sheet: {e}"
        finally:
            if conn:
                conn.close()
        if ROOT is not None:
            ROOT.after(0, lambda: append_status(msg))

    with SYNC_LOCK:
        if SYNC_TIMER is not None:
            SYNC_TIMER.cancel()
        SYNC_TIMER = threading.Timer(delay, worker)
        SYNC_TIMER.daemon = True
        SYNC_TIMER.start()


def password_dialog(parent, conn):
    dialog = tk.Toplevel(parent)
    dialog.title("Acceso restringido")
    dialog.geometry("460x300")
    dialog.resizable(False, False)
    dialog.grab_set()
    result = {"user": None}
    frame = tk.Frame(dialog, bg="white", padx=28, pady=24)
    frame.pack(fill="both", expand=True)
    tk.Label(frame, text="🔒 Zona Administrativa", font=("Segoe UI", 14, "bold"), bg="white", fg="#222222").pack(pady=(0, 8))
    tk.Label(frame, text="Ingresá usuario y contraseña para continuar:", font=("Segoe UI", 10), bg="white", fg="#333333").pack(pady=(0, 14))
    form = tk.Frame(frame, bg="white")
    form.pack()
    user_var = tk.StringVar(value=load_last_user())
    pass_var = tk.StringVar()

    for row, label, var, show in [(0, "Usuario:", user_var, ""), (1, "Contraseña:", pass_var, "*")]:
        tk.Label(form, text=label, font=("Segoe UI", 10), bg="white", fg="#222222").grid(row=row, column=0, sticky="e", padx=(0, 10), pady=7)
        entry = tk.Entry(form, textvariable=var, show=show, width=30, font=("Segoe UI", 10), bg="white", fg="black", insertbackground="black", relief="solid", bd=1)
        entry.grid(row=row, column=1, sticky="w", pady=7)
        if row == 0:
            user_entry = entry
        else:
            pass_entry = entry

    error_label = tk.Label(frame, text="", fg="#b00020", bg="white", font=("Segoe UI", 9))
    error_label.pack(pady=(10, 0))

    def confirm(event=None):
        user = authenticate_adjust_user(conn, user_var.get(), pass_var.get())
        if user:
            result["user"] = user
            save_last_user(user)
            dialog.destroy()
        else:
            error_label.configure(text="Usuario o contraseña incorrectos.")
            pass_var.set("")
            pass_entry.focus_set()

    btns = tk.Frame(frame, bg="white")
    btns.pack(pady=(14, 0))
    tk.Button(btns, text="Ingresar", width=13, font=("Segoe UI", 10, "bold"), bg="#198754", fg="white", relief="flat", command=confirm).pack(side="left", padx=8)
    tk.Button(btns, text="Cancelar", width=13, font=("Segoe UI", 10, "bold"), bg="#6c757d", fg="white", relief="flat", command=dialog.destroy).pack(side="left", padx=8)
    user_entry.bind("<Return>", lambda e: pass_entry.focus_set())
    pass_entry.bind("<Return>", confirm)
    dialog.bind("<Escape>", lambda e: dialog.destroy())
    (pass_entry if user_var.get() else user_entry).focus_set()
    parent.update_idletasks()
    x = parent.winfo_x() + parent.winfo_width() // 2 - 230
    y = parent.winfo_y() + parent.winfo_height() // 2 - 150
    dialog.geometry(f"460x300+{x}+{y}")
    dialog.wait_window()
    return result["user"]


class App:
    def __init__(self, conn):
        global ROOT, STATUS_TEXT
        self.conn = conn
        self.editor_unlocked = False
        self.editor_edited = set()
        self.root = tb.Window(themename="minty")
        ROOT = self.root
        self.root.title(APP_TITLE)
        self.root.geometry("1120x780")
        self.root.minsize(900, 620)
        try:
            self.root.state("zoomed")
        except Exception:
            pass
        container = tb.Frame(self.root, padding=12)
        container.pack(fill="both", expand=True)
        tb.Label(container, text=APP_TITLE, font=("Segoe UI", 22, "bold")).pack(pady=(8, 10))
        self.notebook = ttk.Notebook(container)
        self.notebook.pack(fill="both", expand=True)
        self.tab_bajas = tb.Frame(self.notebook)
        self.tab_stock = tb.Frame(self.notebook, padding=18)
        self.tab_editor = tb.Frame(self.notebook, padding=18)
        self.notebook.add(self.tab_bajas, text="Bajas")
        self.notebook.add(self.tab_stock, text="Stock actual")
        self.notebook.add(self.tab_editor, text="🔒 Ajuste de stock")
        self.build_bajas_tab()
        STATUS_TEXT = self.status_text
        self.build_stock_tab()
        self.build_editor_tab()
        self.bind_events()
        self.refresh_all()
        self.root.protocol("WM_DELETE_WINDOW", self.close)

    def build_bajas_tab(self):
        canvas = tk.Canvas(self.tab_bajas, highlightthickness=0)
        scroll = ttk.Scrollbar(self.tab_bajas, orient="vertical", command=canvas.yview)
        canvas.configure(yscrollcommand=scroll.set)
        scroll.pack(side="right", fill="y")
        canvas.pack(side="left", fill="both", expand=True)
        inner = tb.Frame(canvas, padding=18)
        window = canvas.create_window((0, 0), window=inner, anchor="nw")
        inner.bind("<Configure>", lambda e: canvas.configure(scrollregion=canvas.bbox("all")))
        canvas.bind("<Configure>", lambda e: canvas.itemconfigure(window, width=e.width))
        card = ttk.LabelFrame(inner, text="Datos de la baja", padding=24)
        card.pack(fill="x", padx=30, pady=(10, 18))
        self.motivo_var = tb.StringVar(value="Venta")
        self.cliente_var = tb.StringVar()
        self.prod_var = tb.StringVar()
        self.lote_var = tb.StringVar()
        self.tipo_var = tb.StringVar(value="pallet")
        self.cant_var = tb.StringVar()
        self.obs_var = tb.StringVar()
        motivo_frame = tb.Frame(card)
        motivo_frame.pack(pady=(4, 22))
        tb.Label(motivo_frame, text="Motivo de baja:", font=("Segoe UI", 11, "bold")).pack(side="left", padx=(0, 18))
        for m in MOTIVOS:
            tb.Radiobutton(motivo_frame, text=m, variable=self.motivo_var, value=m, command=self.update_client_visibility).pack(side="left", padx=(0, 16))
        form = tb.Frame(card)
        form.pack(pady=(0, 8))
        self.form = form
        rows = [("Producto:", 0), ("Lote:", 1), ("Cliente:", 2), ("Tipo:", 3), ("Cantidad:", 4), ("Observaciones:", 5)]
        for label, row in rows:
            tb.Label(form, text=label, font=("Segoe UI", 11)).grid(row=row, column=0, sticky="e" if row != 5 else "ne", padx=(0, 14), pady=9)
        self.prod_combo = tb.Combobox(form, textvariable=self.prod_var, width=54, state="readonly")
        self.prod_combo.grid(row=0, column=1, sticky="w", pady=9)
        self.lote_combo = tb.Combobox(form, textvariable=self.lote_var, width=28, state="readonly")
        self.lote_combo.grid(row=1, column=1, sticky="w", pady=9)
        self.disp_var = tb.StringVar(value="Disponible en stock: -")
        tb.Label(form, textvariable=self.disp_var, font=("Segoe UI", 10, "italic"), foreground="#555555").grid(row=1, column=2, sticky="w", padx=(18, 0), pady=9)
        self.cliente_label = tb.Label(form, text="Cliente:", font=("Segoe UI", 11))
        self.cliente_combo = tb.Combobox(form, textvariable=self.cliente_var, width=32, state="normal")
        self.cliente_label.grid(row=2, column=0, sticky="e", padx=(0, 14), pady=9)
        self.cliente_combo.grid(row=2, column=1, sticky="w", pady=9)
        tipo_frame = tb.Frame(form)
        tipo_frame.grid(row=3, column=1, sticky="w", pady=9)
        tb.Radiobutton(tipo_frame, text="Pallets", variable=self.tipo_var, value="pallet").pack(side="left", padx=(0, 18))
        tb.Radiobutton(tipo_frame, text="Packs", variable=self.tipo_var, value="packs").pack(side="left")
        self.cant_entry = tb.Entry(form, textvariable=self.cant_var, width=14, font=("Segoe UI", 11))
        self.cant_entry.grid(row=4, column=1, sticky="w", pady=9)
        self.obs_entry = tb.Entry(form, textvariable=self.obs_var, width=64, font=("Segoe UI", 11))
        self.obs_entry.grid(row=5, column=1, sticky="w", pady=(9, 0))
        self.btn_baja = tb.Button(card, text="ENVIAR BAJA", bootstyle=WARNING, width=24, command=self.submit_baja)
        self.btn_baja.pack(pady=(22, 4))
        status_box = ttk.LabelFrame(inner, text="Estado", padding=12)
        status_box.pack(fill="x", padx=30, pady=(0, 16))
        frame = tb.Frame(status_box)
        frame.pack(fill="both", expand=True)
        status_scroll = ttk.Scrollbar(frame, orient="vertical")
        status_scroll.pack(side="right", fill="y")
        self.status_text = tk.Text(frame, height=7, wrap="word", font=("Segoe UI", 11), yscrollcommand=status_scroll.set)
        self.status_text.pack(side="left", fill="both", expand=True)
        status_scroll.config(command=self.status_text.yview)
        self.status_text.configure(state="disabled")

    def build_stock_tab(self):
        header = tb.Frame(self.tab_stock)
        header.pack(fill="x", pady=(0, 10))
        tb.Label(header, text="Stock actual por producto", font=("Segoe UI", 18, "bold")).pack(side="left")
        tb.Button(header, text="Actualizar stock", bootstyle=INFO, width=18, command=self.refresh_stock).pack(side="right")
        card = ttk.LabelFrame(self.tab_stock, text="Resumen", padding=14)
        card.pack(fill="both", expand=True)
        frame = tb.Frame(card)
        frame.pack(fill="both", expand=True)
        self.stock_tree = ttk.Treeview(frame, columns=("codigo", "descripcion", "pallets", "packs"), show="headings", height=16)
        self.setup_tree(self.stock_tree, [("codigo", "Código", 100, "center"), ("descripcion", "Descripción", 560, "center"), ("pallets", "Pallets", 120, "center"), ("packs", "Packs", 120, "center")])
        scroll = ttk.Scrollbar(frame, orient="vertical", command=self.stock_tree.yview)
        self.stock_tree.configure(yscrollcommand=scroll.set)
        self.stock_tree.pack(side="left", fill="both", expand=True)
        scroll.pack(side="right", fill="y")

    def build_editor_tab(self):
        self.lock_frame = tb.Frame(self.tab_editor)
        self.lock_frame.place(relx=0, rely=0, relwidth=1, relheight=1)
        tb.Label(self.lock_frame, text="🔒", font=("Segoe UI", 48)).pack(pady=(80, 10))
        tb.Label(self.lock_frame, text="Esta sección es de uso administrativo.", font=("Segoe UI", 13)).pack()
        tb.Label(self.lock_frame, text="Hacé clic en el botón para ingresar con contraseña.", font=("Segoe UI", 10), foreground="gray").pack(pady=(4, 20))
        tb.Button(self.lock_frame, text="🔓  Ingresar contraseña", bootstyle=WARNING, width=26, command=self.unlock_editor).pack()
        self.editor_frame = tb.Frame(self.tab_editor)
        header = tb.Frame(self.editor_frame)
        header.pack(fill="x", pady=(0, 10))
        tb.Label(header, text="Ajuste de stock por producto", font=("Segoe UI", 18, "bold")).pack(side="left")
        self.btn_apply = tb.Button(header, text="REALIZAR AJUSTE", bootstyle=DANGER, width=22, command=self.submit_adjustments)
        self.btn_apply.pack(side="right", padx=(6, 0))
        tb.Button(header, text="Actualizar", bootstyle=INFO, width=14, command=self.refresh_editor).pack(side="right", padx=(6, 0))
        tb.Button(header, text="Descartar cambios", bootstyle=SECONDARY, width=18, command=self.refresh_editor).pack(side="right", padx=(6, 0))
        tb.Button(header, text="🔒 Bloquear", bootstyle=SECONDARY, width=14, command=self.lock_editor).pack(side="right")
        tb.Label(self.editor_frame, text="En las columnas de ajuste ingresá el total real contado solo en los productos que quieras modificar.", font=("Segoe UI", 9), foreground="gray").pack(anchor="w", pady=(0, 8))
        card = ttk.LabelFrame(self.editor_frame, text="Productos y stock", padding=10)
        card.pack(fill="both", expand=True)
        frame = tb.Frame(card)
        frame.pack(fill="both", expand=True)
        cols = ("codigo", "descripcion", "pallets_actuales", "packs_actuales", "pallets_nuevos", "packs_nuevos", "diferencia")
        self.editor_tree = ttk.Treeview(frame, columns=cols, show="headings", height=16, selectmode="browse")
        self.setup_tree(self.editor_tree, [
            ("codigo", "Código", 80, "center"), ("descripcion", "Descripción", 420, "w"),
            ("pallets_actuales", "Pallets actuales", 120, "center"), ("packs_actuales", "Packs actuales", 110, "center"),
            ("pallets_nuevos", "Ajuste pallets", 120, "center"), ("packs_nuevos", "Ajuste packs", 110, "center"),
            ("diferencia", "Diferencia", 220, "center"),
        ])
        scroll = ttk.Scrollbar(frame, orient="vertical", command=self.editor_tree.yview)
        self.editor_tree.configure(yscrollcommand=scroll.set)
        self.editor_tree.pack(side="left", fill="both", expand=True)
        scroll.pack(side="right", fill="y")
        panel = ttk.LabelFrame(self.editor_frame, text="Estado de los ajustes", padding=12)
        panel.pack(fill="x", pady=(10, 0))
        self.edit_status_var = tk.StringVar(value="Sin cambios pendientes.")
        tb.Label(panel, textvariable=self.edit_status_var, font=("Segoe UI", 10)).pack(anchor="w")

    @staticmethod
    def setup_tree(tree, columns):
        for key, text, width, anchor in columns:
            tree.heading(key, text=text)
            tree.column(key, width=width, anchor=anchor)

    def bind_events(self):
        self.prod_combo.bind("<<ComboboxSelected>>", lambda e: self.on_product_selected())
        self.lote_combo.bind("<<ComboboxSelected>>", lambda e: self.update_lote_available())
        self.tipo_var.trace_add("write", lambda *_: self.update_lote_available())
        self.obs_entry.bind("<Return>", lambda e: self.submit_baja())
        self.editor_tree.bind("<ButtonRelease-1>", self.edit_editor_cell)
        self.last_tab = 0
        self.notebook.bind("<<NotebookTabChanged>>", self.on_tab_changed)

    def refresh_all(self):
        self.refresh_clients()
        self.refresh_products()
        self.refresh_stock()
        self.update_client_visibility()
        set_status("🟢 Listo para registrar una baja.")
        sync_sheet_async(delay=1.0)

    def refresh_clients(self, selected=None):
        values = get_clients(self.conn)
        self.cliente_combo["values"] = values
        if selected:
            self.cliente_var.set(selected)

    def refresh_products(self):
        current = self.prod_var.get()
        values = [f"{pid} - {desc}" for pid, desc in product_options_with_stock(self.conn)]
        self.prod_combo["values"] = values
        self.prod_var.set(current if current in values else (values[0] if values else ""))
        self.on_product_selected()

    def update_client_visibility(self):
        if self.motivo_var.get() == "Venta":
            self.cliente_label.grid()
            self.cliente_combo.grid()
        else:
            self.cliente_var.set("")
            self.cliente_label.grid_remove()
            self.cliente_combo.grid_remove()

    def selected_product_id(self):
        value = self.prod_var.get().strip()
        if not value:
            raise ValueError("Seleccioná un producto.")
        return int(value.split(" - ")[0])

    def on_product_selected(self):
        try:
            pid = self.selected_product_id()
            lotes = product_lotes(self.conn, pid)
            self.lote_combo["values"] = lotes
            self.lote_var.set(lotes[0] if lotes else "")
        except Exception:
            self.lote_combo["values"] = []
            self.lote_var.set("")
        self.update_lote_available()

    def update_lote_available(self):
        try:
            pid = self.selected_product_id()
            lote = self.lote_var.get().strip()
            if not lote:
                self.disp_var.set("Disponible en stock: -")
                return
            pallets, packs = lote_stock(self.conn, pid, lote)
            if self.tipo_var.get() == "pallet":
                self.disp_var.set(f"Disponible en stock: {pallets} PALLET | Packs: {packs}")
            else:
                self.disp_var.set(f"Disponible en stock: {packs} PACKS | Pallets: {pallets}")
        except Exception as e:
            self.disp_var.set(f"Disponible en stock: error ({e})")

    def clean_form(self):
        self.cant_var.set("")
        self.obs_var.set("")
        if self.motivo_var.get() == "Venta":
            self.cliente_var.set("")
        self.cant_entry.focus_set()

    def submit_baja(self):
        try:
            qty_txt = self.cant_var.get().strip()
            if not qty_txt.isdigit():
                raise ValueError("La cantidad debe ser un número entero.")
            data = baja_manual(
                self.conn,
                self.selected_product_id(),
                self.lote_var.get(),
                self.tipo_var.get(),
                int(qty_txt),
                self.motivo_var.get(),
                self.obs_var.get(),
                self.cliente_var.get(),
            )
            self.refresh_clients(data.get("cliente"))
            self.refresh_products()
            self.refresh_stock()
            self.clean_form()
            series = [str(x) for x in data["series_afectadas"][:12] if x is not None]
            series_text = ", ".join(series) + (", ..." if len(data["series_afectadas"]) > 12 else "") if series else "Sin detalle"
            set_status(
                f"✅ Baja registrada correctamente\n"
                f"IDs bajas: {format_ids(data['bajas_ids'])} | Producto: {data['id_producto']} – {data['descripcion']}\n"
                f"Lote: {data['lote']} | Tipo: {data['tipo_unidad']} | Cantidad: {data['cantidad']}\n"
                f"Motivo: {data['motivo']} | Cliente: {data.get('cliente') or 'No aplica'}\n"
                f"Observaciones: {data.get('observaciones') or ''}\n"
                f"Series afectadas: {series_text}\n"
                f"Stock restante → Pallets: {data['stock_restante_pallets']} | Packs: {data['stock_restante_packs']}\n"
                f"Google Sheet sincronizando automáticamente."
            )
            if data.get("cliente_creado"):
                append_status(f"🆕 Cliente agregado a la base: {data.get('cliente')}")
            send_baja_email_async(data)
            sync_sheet_async(delay=1.0)
        except Exception as e:
            set_status(f"❌ Error al registrar la baja: {e}")

    def refresh_stock(self):
        self.stock_tree.delete(*self.stock_tree.get_children())
        try:
            rows = product_stock_summary(self.conn, only_with_stock=True)
            if not rows:
                self.stock_tree.insert("", "end", values=("-", "Sin stock disponible", 0, 0))
                return
            for row in rows:
                self.stock_tree.insert("", "end", values=row)
        except Exception as e:
            self.stock_tree.insert("", "end", values=("-", f"Error al cargar stock: {e}", "-", "-"))

    def refresh_editor(self):
        self.editor_edited.clear()
        self.editor_tree.delete(*self.editor_tree.get_children())
        try:
            for pid, desc, pallets, packs in product_stock_summary(self.conn, only_with_stock=False):
                tag = "con_stock" if pallets or packs else "sin_stock"
                self.editor_tree.insert("", "end", values=(pid, desc, pallets, packs, 0, 0, "Sin cambios"), tags=(tag,))
            self.editor_tree.tag_configure("con_stock", background="#e8f5e9")
            self.editor_tree.tag_configure("sin_stock", background="#ffffff")
            self.editor_tree.tag_configure("modificado", background="#fff3cd")
            self.editor_tree.tag_configure("error", background="#f8d7da")
            user = f" Usuario: {USUARIO_AJUSTE_ACTUAL}." if USUARIO_AJUSTE_ACTUAL else ""
            self.edit_status_var.set("🟢 Stock cargado. Las columnas de ajuste quedan en 0." + user)
        except Exception as e:
            self.editor_tree.insert("", "end", values=("-", f"Error: {e}", "-", "-", "-", "-", "-"))
            self.edit_status_var.set(f"❌ Error al cargar editor: {e}")

    @staticmethod
    def safe_int(value, field="valor"):
        value = str(value).strip()
        if not value.isdigit():
            raise ValueError(f"El campo {field} debe ser un número entero mayor o igual a 0.")
        return int(value)

    def recalc_row(self, item):
        values = list(self.editor_tree.item(item, "values"))
        try:
            old_p, old_pk = int(values[2]), int(values[3])
            new_p = self.safe_int(values[4], "Ajuste pallets")
            new_pk = self.safe_int(values[5], "Ajuste packs")
            values[6] = format_diff(new_p - old_p, new_pk - old_pk)
            tag = "modificado" if (new_p != old_p or new_pk != old_pk) else ("con_stock" if old_p or old_pk else "sin_stock")
            self.editor_tree.item(item, values=values, tags=(tag,))
            count = len(self.editor_changes(valid=False))
            self.edit_status_var.set(f"✏️ Hay {count} ajuste(s) pendiente(s)." if count else "Sin ajustes pendientes.")
        except Exception as e:
            values[6] = f"Error: {e}"
            self.editor_tree.item(item, values=values, tags=("error",))

    def edit_editor_cell(self, event):
        if self.editor_tree.identify("region", event.x, event.y) != "cell":
            return
        item = self.editor_tree.identify_row(event.y)
        col = self.editor_tree.identify_column(event.x)
        if not item or col not in ("#5", "#6"):
            return
        bbox = self.editor_tree.bbox(item, col)
        if not bbox:
            return
        idx = int(col[1:]) - 1
        x, y, w, h = bbox
        values = list(self.editor_tree.item(item, "values"))
        entry = tk.Entry(self.editor_tree, justify="center", font=("Segoe UI", 10), bg="white", fg="black", insertbackground="black", relief="solid", bd=1)
        entry.insert(0, str(values[idx]))
        entry.select_range(0, "end")
        entry.place(x=x, y=y, width=w, height=h)
        entry.focus_set()

        def save(move_next=False):
            try:
                value = entry.get().strip()
                self.safe_int(value, "ajuste de stock")
                values[idx] = value
                self.editor_tree.item(item, values=values)
                self.editor_edited.add(item)
                self.recalc_row(item)
                entry.destroy()
                if move_next:
                    self.root.after(20, lambda: self.open_next_cell(item, col))
            except Exception as e:
                messagebox.showerror("Valor inválido", str(e), parent=self.root)

        entry.bind("<Return>", lambda e: (save(False), "break")[-1])
        entry.bind("<Tab>", lambda e: (save(True), "break")[-1])
        entry.bind("<FocusOut>", lambda e: save(False))
        entry.bind("<Escape>", lambda e: entry.destroy())

    def open_next_cell(self, item, col):
        rows = list(self.editor_tree.get_children())
        if item not in rows:
            return
        if col == "#5":
            next_item, next_col = item, "#6"
        else:
            idx = rows.index(item) + 1
            if idx >= len(rows):
                return
            next_item, next_col = rows[idx], "#5"
        bbox = self.editor_tree.bbox(next_item, next_col)
        if bbox:
            event = type("Event", (), {"x": bbox[0] + 2, "y": bbox[1] + 2})()
            self.edit_editor_cell(event)

    def editor_changes(self, valid=True):
        changes = []
        for item in self.editor_edited:
            values = list(self.editor_tree.item(item, "values"))
            try:
                pid = int(values[0])
                old_p, old_pk = int(values[2]), int(values[3])
                new_p = self.safe_int(values[4], f"Ajuste pallets del producto {pid}")
                new_pk = self.safe_int(values[5], f"Ajuste packs del producto {pid}")
                if new_p != old_p or new_pk != old_pk:
                    changes.append({"id_producto": pid, "descripcion": str(values[1]), "pallets_actuales": old_p, "packs_actuales": old_pk, "nuevo_pallets": new_p, "nuevo_packs": new_pk})
            except Exception:
                if valid:
                    raise
        return changes

    def confirm_text(self, changes):
        lines = [f"Se van a aplicar {len(changes)} ajuste(s) de stock.", "", "Resumen:", ""]
        for c in changes[:15]:
            lines.append(
                f"[{c['id_producto']}] {c['descripcion']}\n"
                f"  Actual: {c['pallets_actuales']} pallets / {c['packs_actuales']} packs\n"
                f"  Final: {c['nuevo_pallets']} pallets / {c['nuevo_packs']} packs"
            )
        if len(changes) > 15:
            lines.append(f"... y {len(changes) - 15} producto(s) más.")
        lines.extend(["", "Esta acción aplicará todos los cambios juntos y enviará un mail informando las diferencias.", "¿Confirmás continuar?"])
        return "\n".join(lines)

    def submit_adjustments(self):
        user = USUARIO_AJUSTE_ACTUAL
        if not user:
            messagebox.showerror("Usuario no identificado", "Tenés que ingresar con usuario y contraseña para realizar ajustes.", parent=self.root)
            return
        try:
            changes = self.editor_changes(valid=True)
        except Exception as e:
            messagebox.showerror("Valores inválidos", str(e), parent=self.root)
            return
        if not changes:
            messagebox.showinfo("Sin cambios", "No hay cambios pendientes para ajustar.", parent=self.root)
            return
        if not messagebox.askyesno("Confirmar ajuste de stock", self.confirm_text(changes), parent=self.root):
            self.edit_status_var.set("Operación cancelada.")
            return
        try:
            self.btn_apply.configure(state="disabled", text="REALIZANDO AJUSTE...")
            self.root.update_idletasks()
            results = apply_adjustments(self.conn, changes, user)
            if results:
                send_adjustment_email_async(results)
                sync_sheet_async(delay=1.0)
                set_status(f"✅ Ajustes de stock aplicados: {len(results)} producto(s).")
                self.edit_status_var.set(f"✅ Se aplicaron {len(results)} ajuste(s) por {user}.")
            else:
                self.edit_status_var.set("ℹ️ No hubo cambios para aplicar.")
            self.refresh_editor()
            self.refresh_stock()
            self.refresh_products()
        except Exception as e:
            self.edit_status_var.set(f"❌ Error al aplicar ajustes: {e}")
            messagebox.showerror("Error al aplicar ajustes", str(e), parent=self.root)
        finally:
            self.btn_apply.configure(state="normal", text="REALIZAR AJUSTE")

    def lock_editor(self):
        global USUARIO_AJUSTE_ACTUAL
        USUARIO_AJUSTE_ACTUAL = None
        self.editor_unlocked = False
        self.editor_frame.place_forget()
        self.lock_frame.place(relx=0, rely=0, relwidth=1, relheight=1)

    def unlock_editor(self):
        global USUARIO_AJUSTE_ACTUAL
        user = password_dialog(self.root, self.conn)
        if user:
            USUARIO_AJUSTE_ACTUAL = user
            self.editor_unlocked = True
            self.lock_frame.place_forget()
            self.editor_frame.place(relx=0, rely=0, relwidth=1, relheight=1)
            self.refresh_editor()
        else:
            self.notebook.select(0)

    def on_tab_changed(self, event=None):
        current = self.notebook.index(self.notebook.select())
        if self.last_tab == 2 and current != 2:
            self.lock_editor()
        if current == 1:
            self.refresh_stock()
        elif current == 2 and not self.editor_unlocked:
            self.unlock_editor()
        self.last_tab = self.notebook.index(self.notebook.select())

    def close(self):
        global SYNC_TIMER
        try:
            if SYNC_TIMER is not None:
                SYNC_TIMER.cancel()
        except Exception:
            pass
        try:
            self.conn.close()
        except Exception:
            pass
        self.root.destroy()

    def run(self):
        self.root.mainloop()


def main():
    try:
        conn = pg_connect()
        init_tables(conn)
    except Exception as e:
        messagebox.showerror("Error PostgreSQL", f"No se pudo conectar:\n{e}")
        return
    App(conn).run()


if __name__ == "__main__":
    main()
