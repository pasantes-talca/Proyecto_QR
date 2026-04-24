import os
import sys
import json
import urllib.request
import urllib.error
import threading
import tkinter as tk
import unicodedata

import ttkbootstrap as tb
from ttkbootstrap.constants import *
from tkinter import messagebox, ttk

try:
    import psycopg2
except Exception:
    psycopg2 = None


# =======================
#   GLOBAL UI REFS
# =======================
root = None
status_text = None


def set_status(text: str):
    global status_text
    if status_text is None:
        return

    status_text.configure(state="normal")
    status_text.delete("1.0", "end")
    status_text.insert("end", str(text))
    status_text.see("end")
    status_text.configure(state="disabled")


def append_status(text: str):
    global status_text
    if status_text is None:
        return

    current = status_text.get("1.0", "end-1c").strip()

    status_text.configure(state="normal")
    if current:
        status_text.insert("end", "\n" + str(text))
    else:
        status_text.insert("end", str(text))
    status_text.see("end")
    status_text.configure(state="disabled")


# =======================
#   PATHS / CONFIG
# =======================
def get_app_dir():
    if getattr(sys, "frozen", False):
        return os.path.dirname(sys.executable)
    return os.path.dirname(os.path.abspath(__file__))


APP_DIR = get_app_dir()
CONFIG_FILE = os.path.join(APP_DIR, "config.json")


DEFAULT_PG = {
    "host": "10.242.4.13",
    "port": 5432,
    "dbname": "stock",
    "user": "postgres",
    "password": "Talca2025",
    "client_encoding": "WIN1252",
    "schema": "produccion",
    "table_products": "productos",
    "table_stock": "stock",
    "table_bajas": "bajas",
    "table_sheet": "sheet",
    "table_clients": "clientes",
}


CLIENTES_INICIALES = [
    "Rojo",
    "Aiobak",
    "Escudero",
    "Scifo",
    "Abraham",
    "Gatica",
    "Mariano",
    "Ochoa",
    "Martos",
    "Garro",
    "Deposito San Luis",
    "Deposito San Juan",
    "Depósito Salta",
    "Depósito Martins",
    "Preventa",
]


# =======================
#   GOOGLE SHEET WEBAPP
# =======================
SHEETS_WEBAPP_URL = (
    "https://script.google.com/macros/s/"
    "AKfycbwwzMiTB7DEbcOdvi5Vl32xF-McguAlgkzcBQoeAGhzlowc5J1PjF1QLChNcukf5fbn/exec"
)
SHEETS_API_KEY = "TALCA-QR-2026"


def load_config():
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
            return data if isinstance(data, dict) else {}
        except Exception:
            return {}
    return {}


def get_pg_config():
    cfg = DEFAULT_PG.copy()
    data = load_config()

    if isinstance(data.get("pg"), dict):
        for k, v in data["pg"].items():
            if v is not None and v != "":
                cfg[k] = v

    try:
        cfg["port"] = int(cfg["port"])
    except Exception:
        cfg["port"] = 5432

    return cfg


def get_sheet_settings():
    data = load_config()
    sheet = data.get("sheet") if isinstance(data.get("sheet"), dict) else {}
    url = sheet.get("webapp_url") or SHEETS_WEBAPP_URL
    api_key = sheet.get("api_key") or SHEETS_API_KEY
    return url, api_key


# =======================
#   CLIENTES HELPERS
# =======================
def normalize_client_name(nombre: str) -> str:
    return " ".join(str(nombre).strip().split())


def normalize_client_key(nombre: str) -> str:
    nombre = normalize_client_name(nombre)
    nombre = unicodedata.normalize("NFKD", nombre)
    nombre = "".join(ch for ch in nombre if not unicodedata.combining(ch))
    return nombre.casefold()


def get_clientes(conn):
    cfg = get_pg_config()
    schema = cfg["schema"]
    tclients = cfg["table_clients"]

    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT nombre
            FROM {schema}.{tclients}
            WHERE activo = TRUE
            ORDER BY nombre_normalizado ASC, nombre ASC;
        """)
        return [str(r[0]).strip() for r in cur.fetchall()]


def ensure_cliente_exists(conn, nombre_cliente: str):
    cfg = get_pg_config()
    schema = cfg["schema"]
    tclients = cfg["table_clients"]

    nombre = normalize_client_name(nombre_cliente)
    if not nombre:
        raise ValueError("Cliente vacío.")

    nombre_normalizado = normalize_client_key(nombre)

    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT id, nombre, activo
            FROM {schema}.{tclients}
            WHERE nombre_normalizado = %s
            LIMIT 1;
        """, (nombre_normalizado,))
        row = cur.fetchone()

        if row:
            cid = int(row[0])
            nombre_bd = str(row[1]).strip()
            activo = bool(row[2])

            if not activo:
                cur.execute(f"""
                    UPDATE {schema}.{tclients}
                    SET activo = TRUE
                    WHERE id = %s;
                """, (cid,))

            return cid, nombre_bd, False

        cur.execute(f"""
            INSERT INTO {schema}.{tclients} (nombre, nombre_normalizado, activo, fecha_alta)
            VALUES (%s, %s, TRUE, NOW())
            RETURNING id, nombre;
        """, (nombre, nombre_normalizado))
        new_row = cur.fetchone()
        return int(new_row[0]), str(new_row[1]).strip(), True


# =======================
#   DB CONNECT
# =======================
def pg_connect():
    if psycopg2 is None:
        raise RuntimeError("Falta psycopg2. Instalá con: pip install psycopg2-binary")

    cfg = get_pg_config()
    conn = psycopg2.connect(
        host=cfg["host"],
        port=cfg["port"],
        dbname=cfg["dbname"],
        user=cfg["user"],
        password=cfg["password"],
    )
    conn.autocommit = True

    enc = cfg.get("client_encoding")
    if enc:
        conn.set_client_encoding(enc)

    return conn


def init_tables(conn):
    """
    Asegura columnas necesarias en bajas y crea tabla clientes si no existe.
    """
    cfg = get_pg_config()
    schema = cfg["schema"]
    tbajas = cfg["table_bajas"]
    tclients = cfg["table_clients"]

    with conn.cursor() as cur:
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")

        # Columnas en bajas
        for col, definition in [
            ("motivo", "TEXT NOT NULL DEFAULT 'Venta'"),
            ("observaciones", "TEXT"),
            ("tipo_unidad", "TEXT"),
            ("cliente", "TEXT"),
            ("id_cliente", "INTEGER"),
        ]:
            cur.execute(f"""
                DO $$
                BEGIN
                    IF NOT EXISTS (
                        SELECT 1
                        FROM information_schema.columns
                        WHERE table_schema = '{schema}'
                          AND table_name   = '{tbajas}'
                          AND column_name  = '{col}'
                    ) THEN
                        ALTER TABLE {schema}.{tbajas}
                        ADD COLUMN {col} {definition};
                    END IF;
                END $$;
            """)

        # Tabla clientes
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {schema}.{tclients} (
                id SERIAL PRIMARY KEY,
                nombre TEXT NOT NULL,
                nombre_normalizado TEXT NOT NULL,
                activo BOOLEAN NOT NULL DEFAULT TRUE,
                fecha_alta TIMESTAMP NOT NULL DEFAULT NOW()
            );
        """)

        # Asegurar columnas en clientes si la tabla ya existía vieja
        for col, definition in [
            ("nombre", "TEXT"),
            ("nombre_normalizado", "TEXT"),
            ("activo", "BOOLEAN NOT NULL DEFAULT TRUE"),
            ("fecha_alta", "TIMESTAMP NOT NULL DEFAULT NOW()"),
        ]:
            cur.execute(f"""
                DO $$
                BEGIN
                    IF NOT EXISTS (
                        SELECT 1
                        FROM information_schema.columns
                        WHERE table_schema = '{schema}'
                          AND table_name   = '{tclients}'
                          AND column_name  = '{col}'
                    ) THEN
                        ALTER TABLE {schema}.{tclients}
                        ADD COLUMN {col} {definition};
                    END IF;
                END $$;
            """)

        # Unique constraint para evitar duplicados lógicos
        fk_name = f"fk_{tbajas}_{tclients}"
        uq_name = f"uq_{tclients}_nombre_normalizado"

        cur.execute(f"""
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1
                    FROM pg_constraint
                    WHERE conname = '{uq_name}'
                      AND connamespace = '{schema}'::regnamespace
                ) THEN
                    ALTER TABLE {schema}.{tclients}
                    ADD CONSTRAINT {uq_name} UNIQUE (nombre_normalizado);
                END IF;
            END $$;
        """)

        # FK bajas.id_cliente -> clientes.id
        cur.execute(f"""
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1
                    FROM pg_constraint
                    WHERE conname = '{fk_name}'
                      AND connamespace = '{schema}'::regnamespace
                ) THEN
                    ALTER TABLE {schema}.{tbajas}
                    ADD CONSTRAINT {fk_name}
                    FOREIGN KEY (id_cliente)
                    REFERENCES {schema}.{tclients}(id);
                END IF;
            END $$;
        """)

        # Cargar clientes iniciales
        for nombre in CLIENTES_INICIALES:
            nombre_limpio = normalize_client_name(nombre)
            nombre_norm = normalize_client_key(nombre_limpio)
            cur.execute(f"""
                INSERT INTO {schema}.{tclients} (nombre, nombre_normalizado, activo, fecha_alta)
                VALUES (%s, %s, TRUE, NOW())
                ON CONFLICT (nombre_normalizado) DO NOTHING;
            """, (nombre_limpio, nombre_norm))


# =======================
#   GOOGLE SHEET SYNC
# =======================
def _post_json_to_webapp(payload: dict, timeout: int = 30) -> dict:
    url, _ = get_sheet_settings()
    data = json.dumps(payload, ensure_ascii=False).encode("utf-8")

    req = urllib.request.Request(
        url,
        data=data,
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
        try:
            body = e.read().decode("utf-8", errors="ignore")
        except Exception:
            body = ""
        return {
            "ok": False,
            "http_status": getattr(e, "code", None),
            "error": str(e),
            "raw": body,
        }
    except Exception as e:
        return {"ok": False, "error": str(e)}


def send_update_row_to_sheet(id_producto: int, descripcion: str, pallets: int, packs: int) -> dict:
    """
    Envía al Google Sheet el stock actualizado de un producto.

    IMPORTANTE:
    Además de la descripción, ahora se envía el código/id_producto.
    Esto permite que el Apps Script busque el producto por la columna C
    y escriba el stock de pallets en la columna H.
    """
    _, api_key = get_sheet_settings()

    pallets_val = int(pallets or 0)
    packs_val = int(packs or 0)

    payload = {
        "api_key": api_key,
        "action": "scan_pp",
        "type": "scan_pp",

        "codigo": int(id_producto),
        "id_producto": int(id_producto),
        "descripcion": str(descripcion),

        "stock_pallets": pallets_val,
        "stock_packs": packs_val,

        "pallet": pallets_val,
        "bulto": packs_val,
    }

    return _post_json_to_webapp(payload, timeout=20)


def _sync_google_stock_async(id_producto: int, desc: str, net_pallets: int, net_packs: int):
    def _worker():
        warn = ""
        try:
            res = send_update_row_to_sheet(id_producto, desc, net_pallets, net_packs)
            if not (isinstance(res, dict) and res.get("ok") is True):
                warn = f"⚠️ Google Sheet no confirmó OK: {res}"
        except Exception as e:
            warn = f"⚠️ Error al sync con Google Sheet: {e}"

        if warn and root is not None:
            try:
                root.after(0, lambda: append_status(warn))
            except Exception:
                pass

    threading.Thread(target=_worker, daemon=True).start()


# =======================
#   DB HELPERS
# =======================
def get_products_with_stock(conn):
    cfg = get_pg_config()
    schema = cfg["schema"]
    tstock = cfg["table_stock"]
    tprod = cfg["table_products"]

    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT p.id, p.descripcion
            FROM {schema}.{tprod} p
            WHERE EXISTS (
                SELECT 1
                FROM {schema}.{tstock} s
                WHERE s.id_producto = p.id
                  AND (
                        UPPER(BTRIM(COALESCE(s.tipo_unidad, ''))) IN ('PALLET', 'PALLETS')
                        OR (
                            UPPER(BTRIM(COALESCE(s.tipo_unidad, ''))) = 'PACKS'
                            AND COALESCE(s.packs, 0) > 0
                        )
                  )
            )
            ORDER BY p.id ASC;
        """)
        return cur.fetchall()


def get_lotes_for_product(conn, id_producto: int):
    cfg = get_pg_config()
    schema = cfg["schema"]
    tstock = cfg["table_stock"]

    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT DISTINCT lote
            FROM {schema}.{tstock}
            WHERE id_producto = %s
              AND (
                    UPPER(BTRIM(COALESCE(tipo_unidad, ''))) IN ('PALLET', 'PALLETS')
                    OR (
                        UPPER(BTRIM(COALESCE(tipo_unidad, ''))) = 'PACKS'
                        AND COALESCE(packs, 0) > 0
                    )
              )
            ORDER BY lote ASC;
        """, (int(id_producto),))
        return [r[0] for r in cur.fetchall()]


def get_stock_summary_by_product(conn):
    cfg = get_pg_config()
    schema = cfg["schema"]
    tprod = cfg["table_products"]
    tstock = cfg["table_stock"]

    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT
                p.id,
                p.descripcion,
                COALESCE(SUM(
                    CASE
                        WHEN UPPER(BTRIM(COALESCE(s.tipo_unidad, ''))) IN ('PALLET', 'PALLETS')
                        THEN 1
                        ELSE 0
                    END
                ), 0) AS pallets,
                COALESCE(SUM(
                    CASE
                        WHEN UPPER(BTRIM(COALESCE(s.tipo_unidad, ''))) = 'PACKS'
                        THEN COALESCE(s.packs, 0)
                        ELSE 0
                    END
                ), 0) AS packs
            FROM {schema}.{tprod} p
            LEFT JOIN {schema}.{tstock} s
              ON s.id_producto = p.id
            GROUP BY p.id, p.descripcion
            HAVING
                COALESCE(SUM(
                    CASE
                        WHEN UPPER(BTRIM(COALESCE(s.tipo_unidad, ''))) IN ('PALLET', 'PALLETS')
                        THEN 1
                        ELSE 0
                    END
                ), 0) > 0
                OR
                COALESCE(SUM(
                    CASE
                        WHEN UPPER(BTRIM(COALESCE(s.tipo_unidad, ''))) = 'PACKS'
                        THEN COALESCE(s.packs, 0)
                        ELSE 0
                    END
                ), 0) > 0
            ORDER BY p.id ASC;
        """)
        rows = cur.fetchall()

    result = []
    for row in rows:
        pid = int(row[0])
        desc = str(row[1]).strip() if row[1] else "Sin descripción"
        pallets = int(row[2] or 0)
        packs = int(row[3] or 0)
        result.append((pid, desc, pallets, packs))
    return result


def compute_net_available_lote(conn, id_producto: int, lote: str):
    cfg = get_pg_config()
    schema = cfg["schema"]
    tstock = cfg["table_stock"]

    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT
                COALESCE(SUM(
                    CASE
                        WHEN UPPER(BTRIM(COALESCE(tipo_unidad, ''))) IN ('PALLET', 'PALLETS')
                        THEN 1
                        ELSE 0
                    END
                ), 0) AS pallets,
                COALESCE(SUM(
                    CASE
                        WHEN UPPER(BTRIM(COALESCE(tipo_unidad, ''))) = 'PACKS'
                        THEN COALESCE(packs, 0)
                        ELSE 0
                    END
                ), 0) AS packs
            FROM {schema}.{tstock}
            WHERE id_producto = %s
              AND BTRIM(COALESCE(lote::text, '')) = BTRIM(%s);
        """, (int(id_producto), str(lote)))
        row = cur.fetchone()
        return int(row[0] or 0), int(row[1] or 0)


def get_product_net_stock(conn, id_producto: int):
    cfg = get_pg_config()
    schema = cfg["schema"]
    tprod = cfg["table_products"]
    tstock = cfg["table_stock"]

    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT
                p.descripcion,
                COALESCE(SUM(
                    CASE
                        WHEN UPPER(BTRIM(COALESCE(s.tipo_unidad, ''))) IN ('PALLET', 'PALLETS')
                        THEN 1
                        ELSE 0
                    END
                ), 0) AS pallets,
                COALESCE(SUM(
                    CASE
                        WHEN UPPER(BTRIM(COALESCE(s.tipo_unidad, ''))) = 'PACKS'
                        THEN COALESCE(s.packs, 0)
                        ELSE 0
                    END
                ), 0) AS packs
            FROM {schema}.{tprod} p
            LEFT JOIN {schema}.{tstock} s ON s.id_producto = p.id
            WHERE p.id = %s
            GROUP BY p.descripcion;
        """, (int(id_producto),))
        row = cur.fetchone()
        if row and row[0]:
            return str(row[0]).strip(), int(row[1] or 0), int(row[2] or 0)
        return "Sin descripción", 0, 0


# =======================
#   ELIMINAR DE STOCK
# =======================
def delete_from_stock_iterative(conn, id_producto: int, lote: str, tipo_unidad: str, cantidad: int):
    """
    Descuenta stock real de forma iterativa y segura.
    Devuelve el detalle de series afectadas.
    """
    cfg = get_pg_config()
    schema = cfg["schema"]
    tstock = cfg["table_stock"]

    tipo_unidad = str(tipo_unidad).upper().strip()
    cantidad = int(cantidad)
    afectadas = []

    with conn.cursor() as cur:
        if tipo_unidad == "PALLET":
            cur.execute(f"""
                SELECT ctid::text, nro_serie
                FROM {schema}.{tstock}
                WHERE id_producto = %s
                  AND BTRIM(COALESCE(lote::text, '')) = BTRIM(%s)
                  AND UPPER(BTRIM(COALESCE(tipo_unidad, ''))) IN ('PALLET', 'PALLETS')
                ORDER BY nro_serie ASC
                LIMIT %s
                FOR UPDATE;
            """, (int(id_producto), str(lote), int(cantidad)))
            rows = cur.fetchall()

            if len(rows) < cantidad:
                raise ValueError(f"No hay pallets suficientes en ese lote. Disponibles: {len(rows)}")

            for ctid_txt, nro_serie in rows:
                cur.execute(f"""
                    DELETE FROM {schema}.{tstock}
                    WHERE ctid = %s::tid;
                """, (ctid_txt,))
                if cur.rowcount != 1:
                    raise ValueError(f"No se pudo eliminar correctamente el pallet serie {nro_serie}.")
                afectadas.append(str(nro_serie))

        elif tipo_unidad == "PACKS":
            cur.execute(f"""
                SELECT ctid::text, nro_serie, COALESCE(packs, 0)
                FROM {schema}.{tstock}
                WHERE id_producto = %s
                  AND BTRIM(COALESCE(lote::text, '')) = BTRIM(%s)
                  AND UPPER(BTRIM(COALESCE(tipo_unidad, ''))) = 'PACKS'
                  AND COALESCE(packs, 0) > 0
                ORDER BY nro_serie ASC
                FOR UPDATE;
            """, (int(id_producto), str(lote)))
            rows = cur.fetchall()

            remaining = int(cantidad)
            disponibles = sum(int(r[2] or 0) for r in rows)

            if disponibles < remaining:
                raise ValueError(f"No hay packs suficientes en ese lote. Disponibles: {disponibles}")

            for ctid_txt, nro_serie, packs_val in rows:
                if remaining <= 0:
                    break

                packs_val = int(packs_val or 0)
                if packs_val <= 0:
                    continue

                if packs_val <= remaining:
                    cur.execute(f"""
                        DELETE FROM {schema}.{tstock}
                        WHERE ctid = %s::tid;
                    """, (ctid_txt,))
                    if cur.rowcount != 1:
                        raise ValueError(f"No se pudo eliminar correctamente el registro packs serie {nro_serie}.")
                    remaining -= packs_val
                    afectadas.append(f"{nro_serie} (-{packs_val} packs)")
                else:
                    cur.execute(f"""
                        UPDATE {schema}.{tstock}
                        SET packs = packs - %s
                        WHERE ctid = %s::tid;
                    """, (remaining, ctid_txt))
                    if cur.rowcount != 1:
                        raise ValueError(f"No se pudo actualizar correctamente el registro packs serie {nro_serie}.")
                    afectadas.append(f"{nro_serie} (-{remaining} packs)")
                    remaining = 0

            if remaining > 0:
                raise ValueError("No se pudo completar la baja iterativa de packs.")
        else:
            raise ValueError("Tipo de unidad inválido para eliminación iterativa.")

    return afectadas


# =======================
#   SHEET / UPSERT
# =======================
def upsert_sheet(conn, id_producto: int, stock_pallets: int, stock_packs: int):
    cfg = get_pg_config()
    schema = cfg["schema"]
    tsheet = cfg["table_sheet"]

    with conn.cursor() as cur:
        cur.execute(f"""
            INSERT INTO {schema}.{tsheet}(id_producto, stock_pallets, stock_packs)
            VALUES (%s, %s, %s)
            ON CONFLICT (id_producto)
            DO UPDATE SET stock_pallets = EXCLUDED.stock_pallets,
                          stock_packs   = EXCLUDED.stock_packs;
        """, (int(id_producto), int(stock_pallets), int(stock_packs)))


def registrar_baja(conn, id_producto: int, lote: str, cantidad: int, motivo: str,
                   observaciones: str = None, tipo_unidad: str = None,
                   cliente: str = None, id_cliente: int = None):
    cfg = get_pg_config()
    schema = cfg["schema"]
    tbajas = cfg["table_bajas"]

    with conn.cursor() as cur:
        cur.execute(f"""
            INSERT INTO {schema}.{tbajas} (
                id_producto, stock_lote, fecha_hora, cantidad,
                motivo, observaciones, tipo_unidad, cliente, id_cliente
            ) VALUES (
                %s, %s, NOW(), %s, %s, %s, %s, %s, %s
            )
            RETURNING id;
        """, (
            int(id_producto),
            str(lote),
            int(cantidad),
            str(motivo),
            (observaciones.strip() if observaciones else None),
            (str(tipo_unidad).upper().strip() if tipo_unidad else None),
            (cliente.strip() if cliente else None),
            (int(id_cliente) if id_cliente is not None else None),
        ))
        return cur.fetchone()[0]


# =======================
#   BAJA MANUAL
# =======================
def baja_manual(conn, id_producto: int, lote: str, tipo: str, cantidad: int,
                motivo: str, observaciones: str = None, cliente: str = None):
    pid = int(id_producto)
    lote = str(lote).strip()
    tipo = (tipo or "").strip().lower()
    cantidad = int(cantidad)
    motivo = str(motivo).strip()

    if tipo not in ("pallet", "packs"):
        raise ValueError("Tipo inválido (pallet / packs).")
    if cantidad <= 0:
        raise ValueError("Cantidad debe ser > 0.")

    if motivo == "Venta" and not (cliente and cliente.strip()):
        raise ValueError("Debes seleccionar o escribir un cliente para la baja por Venta.")

    tipo_unidad = "PALLET" if tipo == "pallet" else "PACKS"

    prev_autocommit = conn.autocommit
    id_cliente = None
    cliente_final = None
    cliente_creado = False

    try:
        conn.autocommit = False

        if motivo == "Venta":
            id_cliente, cliente_final, cliente_creado = ensure_cliente_exists(conn, cliente)

        net_pallets_lote, net_packs_lote = compute_net_available_lote(conn, pid, lote)

        if tipo_unidad == "PALLET" and net_pallets_lote < cantidad:
            raise ValueError(f"No hay pallets suficientes en ese lote. Disponibles: {net_pallets_lote}")

        if tipo_unidad == "PACKS" and net_packs_lote < cantidad:
            raise ValueError(f"No hay packs suficientes en ese lote. Disponibles: {net_packs_lote}")

        series_afectadas = delete_from_stock_iterative(conn, pid, lote, tipo_unidad, cantidad)

        baja_id = registrar_baja(
            conn,
            pid,
            lote,
            cantidad,
            motivo,
            observaciones,
            tipo_unidad=tipo_unidad,
            cliente=cliente_final,
            id_cliente=id_cliente
        )

        desc, net_pallets, net_packs = get_product_net_stock(conn, pid)
        upsert_sheet(conn, pid, net_pallets, net_packs)

        conn.commit()

    except Exception:
        conn.rollback()
        raise

    finally:
        conn.autocommit = prev_autocommit

    _sync_google_stock_async(pid, desc, net_pallets, net_packs)

    return (
        baja_id,
        pid,
        desc,
        lote,
        tipo_unidad,
        cantidad,
        net_pallets,
        net_packs,
        series_afectadas,
        cliente_final,
        cliente_creado
    )


# =======================
#   UI
# =======================
MOTIVOS = ("Venta", "Calidad", "Desarme", "Observacion")


def main():
    global root, status_text

    try:
        conn = pg_connect()
        init_tables(conn)
    except Exception as e:
        messagebox.showerror("Error PostgreSQL", f"No se pudo conectar:\n{e}")
        return

    root = tb.Window(themename="minty")
    root.title("Baja manual – Talca")
    root.geometry("1120x780")
    root.minsize(900, 620)

    try:
        root.state("zoomed")
    except Exception:
        pass

    container = tb.Frame(root, padding=12)
    container.pack(fill="both", expand=True)

    tb.Label(
        container,
        text="Sistema de Bajas – Talca",
        font=("Segoe UI", 22, "bold")
    ).pack(pady=(8, 10))

    notebook = ttk.Notebook(container)
    notebook.pack(fill="both", expand=True)

    # =========================================================
    # TAB BAJAS CON SCROLL
    # =========================================================
    tab_bajas = tb.Frame(notebook)
    tab_stock = tb.Frame(notebook, padding=18)

    notebook.add(tab_bajas, text="Bajas")
    notebook.add(tab_stock, text="Stock actual")

    bajas_canvas = tk.Canvas(tab_bajas, highlightthickness=0)
    bajas_scrollbar = ttk.Scrollbar(tab_bajas, orient="vertical", command=bajas_canvas.yview)
    bajas_canvas.configure(yscrollcommand=bajas_scrollbar.set)

    bajas_scrollbar.pack(side="right", fill="y")
    bajas_canvas.pack(side="left", fill="both", expand=True)

    bajas_inner = tb.Frame(bajas_canvas, padding=18)
    canvas_window = bajas_canvas.create_window((0, 0), window=bajas_inner, anchor="nw")

    def _update_bajas_scrollregion(event=None):
        bajas_canvas.configure(scrollregion=bajas_canvas.bbox("all"))

    def _resize_bajas_inner(event):
        bajas_canvas.itemconfigure(canvas_window, width=event.width)

    bajas_inner.bind("<Configure>", _update_bajas_scrollregion)
    bajas_canvas.bind("<Configure>", _resize_bajas_inner)

    def _on_mousewheel(event):
        try:
            bajas_canvas.yview_scroll(int(-1 * (event.delta / 120)), "units")
        except Exception:
            pass

    def _bind_mousewheel(event=None):
        try:
            bajas_canvas.bind_all("<MouseWheel>", _on_mousewheel)
        except Exception:
            pass

    def _unbind_mousewheel(event=None):
        try:
            bajas_canvas.unbind_all("<MouseWheel>")
        except Exception:
            pass

    bajas_canvas.bind("<Enter>", _bind_mousewheel)
    bajas_canvas.bind("<Leave>", _unbind_mousewheel)
    bajas_inner.bind("<Enter>", _bind_mousewheel)
    bajas_inner.bind("<Leave>", _unbind_mousewheel)

    # =========================================================
    # PESTAÑA BAJAS
    # =========================================================
    card_baja = ttk.LabelFrame(bajas_inner, text="Datos de la baja", padding=24)
    card_baja.pack(fill="x", padx=30, pady=(10, 18))

    motivo_var = tb.StringVar(value="Venta")
    cliente_var = tb.StringVar()
    prod_var = tb.StringVar()
    lote_var = tb.StringVar()
    type_var = tb.StringVar(value="pallet")
    cant_var = tb.StringVar(value="")
    obs_manual_var = tb.StringVar()

    frame_motivo = tb.Frame(card_baja)
    frame_motivo.pack(pady=(4, 22))

    motivo_inner = tb.Frame(frame_motivo)
    motivo_inner.pack()

    tb.Label(
        motivo_inner,
        text="Motivo de baja:",
        font=("Segoe UI", 11, "bold")
    ).pack(side="left", padx=(0, 18))

    def refresh_client_combo_values(selected_text=None):
        values = get_clientes(conn)
        cliente_combo["values"] = values
        if selected_text is not None:
            cliente_var.set(selected_text)

    def update_cliente_visibility():
        if motivo_var.get() == "Venta":
            cliente_label.grid()
            cliente_combo.grid()
        else:
            cliente_var.set("")
            cliente_label.grid_remove()
            cliente_combo.grid_remove()

    for m in MOTIVOS:
        tb.Radiobutton(
            motivo_inner,
            text=m,
            variable=motivo_var,
            value=m,
            command=update_cliente_visibility
        ).pack(side="left", padx=(0, 16))

    form_wrap = tb.Frame(card_baja)
    form_wrap.pack(pady=(0, 8))

    tb.Label(form_wrap, text="Producto:", font=("Segoe UI", 11)).grid(
        row=0, column=0, sticky="e", padx=(0, 14), pady=9
    )

    prods = get_products_with_stock(conn)
    options = [f"{pid} - {desc}" for pid, desc in prods]

    prod_combo = tb.Combobox(
        form_wrap,
        textvariable=prod_var,
        values=options,
        width=48,
        state="readonly"
    )
    prod_combo.grid(row=0, column=1, sticky="w", pady=9)

    tb.Label(form_wrap, text="Lote:", font=("Segoe UI", 11)).grid(
        row=1, column=0, sticky="e", padx=(0, 14), pady=9
    )

    lote_combo = tb.Combobox(
        form_wrap,
        textvariable=lote_var,
        width=28,
        state="readonly"
    )
    lote_combo.grid(row=1, column=1, sticky="w", pady=9)

    cliente_label = tb.Label(form_wrap, text="Cliente:", font=("Segoe UI", 11))
    cliente_combo = tb.Combobox(
        form_wrap,
        textvariable=cliente_var,
        values=get_clientes(conn),
        width=32,
        state="normal"
    )
    cliente_label.grid(row=2, column=0, sticky="e", padx=(0, 14), pady=9)
    cliente_combo.grid(row=2, column=1, sticky="w", pady=9)

    tb.Label(form_wrap, text="Tipo:", font=("Segoe UI", 11)).grid(
        row=3, column=0, sticky="e", padx=(0, 14), pady=9
    )

    tipo_frame = tb.Frame(form_wrap)
    tipo_frame.grid(row=3, column=1, sticky="w", pady=9)

    tb.Radiobutton(
        tipo_frame,
        text="Pallets",
        variable=type_var,
        value="pallet"
    ).pack(side="left", padx=(0, 18))

    tb.Radiobutton(
        tipo_frame,
        text="Packs",
        variable=type_var,
        value="packs"
    ).pack(side="left")

    tb.Label(form_wrap, text="Cantidad:", font=("Segoe UI", 11)).grid(
        row=4, column=0, sticky="e", padx=(0, 14), pady=9
    )

    cant_entry = tb.Entry(form_wrap, textvariable=cant_var, width=14, font=("Segoe UI", 11))
    cant_entry.grid(row=4, column=1, sticky="w", pady=9)

    tb.Label(form_wrap, text="Observaciones:", font=("Segoe UI", 11)).grid(
        row=5, column=0, sticky="ne", padx=(0, 14), pady=(9, 0)
    )

    obs_entry = tb.Entry(form_wrap, textvariable=obs_manual_var, width=64, font=("Segoe UI", 11))
    obs_entry.grid(row=5, column=1, sticky="w", pady=(9, 0))

    actions_frame = tb.Frame(card_baja)
    actions_frame.pack(pady=(22, 4))

    btn_send = tb.Button(
        actions_frame,
        text="ENVIAR BAJA",
        bootstyle=WARNING,
        width=24
    )
    btn_send.pack()

    # Estado con scroll propio
    status_box = ttk.LabelFrame(bajas_inner, text="Estado", padding=12)
    status_box.pack(fill="x", padx=30, pady=(0, 16))

    status_frame = tb.Frame(status_box)
    status_frame.pack(fill="both", expand=True)

    status_scrollbar = ttk.Scrollbar(status_frame, orient="vertical")
    status_scrollbar.pack(side="right", fill="y")

    status_text = tk.Text(
        status_frame,
        height=7,
        wrap="word",
        font=("Segoe UI", 11),
        yscrollcommand=status_scrollbar.set
    )
    status_text.pack(side="left", fill="both", expand=True)
    status_scrollbar.config(command=status_text.yview)
    status_text.configure(state="disabled")

    set_status("🟢 Listo para registrar una baja.")

    # =========================================================
    # PESTAÑA STOCK
    # =========================================================
    header_stock = tb.Frame(tab_stock)
    header_stock.pack(fill="x", pady=(0, 10))

    tb.Label(
        header_stock,
        text="Stock actual por producto",
        font=("Segoe UI", 18, "bold")
    ).pack(side="left")

    btn_refresh_stock = tb.Button(
        header_stock,
        text="Actualizar stock",
        bootstyle=INFO,
        width=18
    )
    btn_refresh_stock.pack(side="right")

    stock_card = ttk.LabelFrame(tab_stock, text="Resumen", padding=14)
    stock_card.pack(fill="both", expand=True)

    stock_table_frame = tb.Frame(stock_card)
    stock_table_frame.pack(fill="both", expand=True)

    stock_tree = ttk.Treeview(
        stock_table_frame,
        columns=("codigo", "descripcion", "pallets", "packs"),
        show="headings",
        height=16
    )

    stock_tree.heading("codigo", text="Código")
    stock_tree.heading("descripcion", text="Descripción")
    stock_tree.heading("pallets", text="Pallets")
    stock_tree.heading("packs", text="Packs")

    stock_tree.column("codigo", width=100, anchor="center")
    stock_tree.column("descripcion", width=560, anchor="w")
    stock_tree.column("pallets", width=120, anchor="center")
    stock_tree.column("packs", width=120, anchor="center")

    stock_scroll = ttk.Scrollbar(stock_table_frame, orient="vertical", command=stock_tree.yview)
    stock_tree.configure(yscrollcommand=stock_scroll.set)

    stock_tree.pack(side="left", fill="both", expand=True)
    stock_scroll.pack(side="right", fill="y")

    # =========================================================
    # FUNCIONES UI
    # =========================================================
    def refresh_stock_box():
        for item in stock_tree.get_children():
            stock_tree.delete(item)

        try:
            rows = get_stock_summary_by_product(conn)

            if not rows:
                stock_tree.insert("", "end", values=("-", "Sin stock disponible", 0, 0))
                return

            for pid, desc, pallets, packs in rows:
                stock_tree.insert("", "end", values=(pid, desc, pallets, packs))
        except Exception as e:
            stock_tree.insert("", "end", values=("-", f"Error al cargar stock: {e}", "-", "-"))

    def on_product_select(event=None):
        val = prod_var.get()
        if not val:
            lote_combo["values"] = []
            lote_var.set("")
            return

        try:
            pid = int(val.split(" - ")[0])
            lotes = get_lotes_for_product(conn, pid)
            lote_combo["values"] = lotes
            lote_var.set(lotes[0] if lotes else "")
        except Exception:
            lote_combo["values"] = []
            lote_var.set("")

    prod_combo.bind("<<ComboboxSelected>>", on_product_select)

    def refresh_product_combo():
        current_value = prod_var.get()

        new_prods = get_products_with_stock(conn)
        new_options = [f"{pid} - {desc}" for pid, desc in new_prods]
        prod_combo["values"] = new_options

        if current_value in new_options:
            prod_var.set(current_value)
        elif new_options:
            prod_var.set(new_options[0])
        else:
            prod_var.set("")

        on_product_select()

    def limpiar_formulario():
        cant_var.set("")
        obs_manual_var.set("")
        if motivo_var.get() == "Venta":
            cliente_var.set("")
        cant_entry.focus_set()

    def submit_manual():
        try:
            pstr = prod_var.get()
            if not pstr:
                raise ValueError("Seleccioná un producto.")
            pid = int(pstr.split(" - ")[0])

            lote = lote_var.get().strip()
            if not lote:
                raise ValueError("Seleccioná un lote.")

            tipo = type_var.get()
            qty_str = cant_var.get().strip()
            if not qty_str.isdigit():
                raise ValueError("La cantidad debe ser un número entero.")
            qty = int(qty_str)
            if qty <= 0:
                raise ValueError("La cantidad debe ser mayor que 0.")

            motivo = motivo_var.get()
            obs = obs_manual_var.get().strip() or None
            cliente = normalize_client_name(cliente_var.get()) or None

            if motivo == "Venta" and not cliente:
                raise ValueError("Debes seleccionar o escribir un cliente cuando el motivo es Venta.")

            (
                baja_id, pid, desc, lote, tipo_unidad,
                cantidad, net_p, net_pk, series_afectadas,
                cliente_final, cliente_creado
            ) = baja_manual(
                conn, pid, lote, tipo, qty, motivo, obs, cliente
            )

            refresh_client_combo_values(cliente_final)

            obs_txt = obs if obs else "Ninguna"
            cliente_txt = cliente_final if cliente_final else "No aplica"

            if series_afectadas:
                detalle_series = ", ".join(series_afectadas[:12])
                if len(series_afectadas) > 12:
                    detalle_series += ", ..."
            else:
                detalle_series = "Sin detalle"

            msg = (
                f"✅ Baja registrada correctamente\n"
                f"ID baja: {baja_id} | Producto: {pid} – {desc}\n"
                f"Lote: {lote} | Tipo: {tipo_unidad} | Cantidad: {cantidad}\n"
                f"Motivo: {motivo} | Cliente: {cliente_txt}\n"
                f"Observaciones: {obs_txt}\n"
                f"Series afectadas: {detalle_series}\n"
                f"Stock restante → Pallets: {net_p} | Packs: {net_pk}"
            )

            if cliente_creado:
                msg += f"\n🆕 Cliente agregado a la base: {cliente_final}"

            set_status(msg)

            refresh_product_combo()
            refresh_stock_box()
            limpiar_formulario()

        except Exception as e:
            set_status(f"❌ Error al registrar la baja: {e}")

    def on_tab_changed(event=None):
        try:
            current_index = notebook.index(notebook.select())
            if current_index == 1:
                refresh_stock_box()
        except Exception:
            pass

    btn_send.configure(command=submit_manual)
    btn_refresh_stock.configure(command=refresh_stock_box)
    obs_entry.bind("<Return>", lambda e: submit_manual())
    notebook.bind("<<NotebookTabChanged>>", on_tab_changed)

    if options:
        prod_var.set(options[0])
        on_product_select()

    refresh_client_combo_values()
    update_cliente_visibility()
    refresh_stock_box()
    root.mainloop()


if __name__ == "__main__":
    main()