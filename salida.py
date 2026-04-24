import os
import sys
import json
import urllib.request
import urllib.error
import threading
import tkinter as tk
import unicodedata
import smtplib
from datetime import datetime
from email.message import EmailMessage

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

PASSWORD_ADMIN = "Talca2026**"
EMAIL_AJUSTES_TO = "psantes@talca.com.ar"
LOTE_AJUSTE_DEFAULT = "AJUSTE"


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
    "dbname": "stock_copia",
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


def get_email_settings():
    """
    Para que el mail salga realmente, agregá esto en config.json:

    {
      "email": {
        "smtp_host": "smtp.office365.com",
        "smtp_port": 587,
        "smtp_user": "usuario@talca.com",
        "smtp_password": "CLAVE_DEL_CORREO",
        "from": "usuario@talca.com",
        "to": "psantes@talca.com",
        "use_tls": true
      }
    }
    """
    data = load_config()
    email = data.get("email") if isinstance(data.get("email"), dict) else {}

    return {
        "smtp_host": email.get("smtp_host") or email.get("host"),
        "smtp_port": int(email.get("smtp_port") or email.get("port") or 587),
        "smtp_user": email.get("smtp_user") or email.get("user"),
        "smtp_password": email.get("smtp_password") or email.get("password"),
        "from": email.get("from") or email.get("from_addr") or email.get("smtp_user") or email.get("user"),
        "to": email.get("to") or EMAIL_AJUSTES_TO,
        "use_tls": bool(email.get("use_tls", True)),
    }


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
    cfg = get_pg_config()
    schema = cfg["schema"]
    tbajas = cfg["table_bajas"]
    tclients = cfg["table_clients"]

    with conn.cursor() as cur:
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")

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

        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {schema}.{tclients} (
                id SERIAL PRIMARY KEY,
                nombre TEXT NOT NULL,
                nombre_normalizado TEXT NOT NULL,
                activo BOOLEAN NOT NULL DEFAULT TRUE,
                fecha_alta TIMESTAMP NOT NULL DEFAULT NOW()
            );
        """)

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
#   EMAIL
# =======================
def build_stock_changes_email_body(cambios: list[dict]) -> str:
    now = datetime.now().strftime("%d/%m/%Y %H:%M:%S")

    lines = [
        "Se realizaron ajustes de stock desde el sistema de bajas de Talca.",
        "",
        f"Fecha y hora: {now}",
        f"Cantidad de productos modificados: {len(cambios)}",
        "",
        "Detalle de cambios:",
        "",
    ]

    for c in cambios:
        lines.append(f"Producto: [{c['id_producto']}] {c['descripcion']}")
        lines.append(f"Stock anterior: {c['pallets_antes']} pallets / {c['packs_antes']} packs")
        lines.append(f"Stock nuevo: {c['pallets_despues']} pallets / {c['packs_despues']} packs")
        lines.append(f"Diferencia: {format_diff(c['diff_pallets'], c['diff_packs'])}")

        bajas_ids = c.get("bajas_ids") or []
        if bajas_ids:
            lines.append(f"Bajas registradas: {', '.join(str(x) for x in bajas_ids)}")
        else:
            lines.append("Bajas registradas: No corresponde")

        lines.append("-" * 60)

    return "\n".join(lines)


def send_stock_changes_email(cambios: list[dict]):
    settings = get_email_settings()

    host = settings["smtp_host"]
    port = settings["smtp_port"]
    user = settings["smtp_user"]
    password = settings["smtp_password"]
    from_addr = settings["from"]
    to_addr = settings["to"]
    use_tls = settings["use_tls"]

    if not host or not user or not password or not from_addr or not to_addr:
        raise ValueError(
            "Falta configurar el email en config.json. "
            "Necesitás smtp_host, smtp_user, smtp_password, from y to."
        )

    msg = EmailMessage()
    msg["Subject"] = f"Ajustes de stock Talca - {datetime.now().strftime('%d/%m/%Y %H:%M')}"
    msg["From"] = from_addr
    msg["To"] = to_addr
    msg.set_content(build_stock_changes_email_body(cambios))

    with smtplib.SMTP(host, port, timeout=30) as smtp:
        if use_tls:
            smtp.starttls()
        smtp.login(user, password)
        smtp.send_message(msg)


def send_stock_changes_email_async(cambios: list[dict]):
    def _worker():
        try:
            send_stock_changes_email(cambios)
            msg = f"📧 Mail enviado correctamente a {EMAIL_AJUSTES_TO}."
        except Exception as e:
            msg = f"⚠️ No se pudo enviar el mail de ajustes: {e}"

        if root is not None:
            try:
                root.after(0, lambda: append_status(msg))
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


def get_stock_summary_all_products(conn):
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
#   STOCK LOW LEVEL
# =======================
def get_next_nro_serie(conn, id_producto: int):
    cfg = get_pg_config()
    schema = cfg["schema"]
    tstock = cfg["table_stock"]

    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT COALESCE(MAX(nro_serie), 0)
            FROM {schema}.{tstock}
            WHERE id_producto = %s;
        """, (int(id_producto),))
        row = cur.fetchone()

    return int(row[0] or 0) + 1


def get_lote_para_alta(conn, id_producto: int):
    cfg = get_pg_config()
    schema = cfg["schema"]
    tstock = cfg["table_stock"]

    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT lote
            FROM {schema}.{tstock}
            WHERE id_producto = %s
            ORDER BY nro_serie DESC NULLS LAST
            LIMIT 1;
        """, (int(id_producto),))
        row = cur.fetchone()

    if row and row[0]:
        return str(row[0]).strip()

    return LOTE_AJUSTE_DEFAULT


def insert_stock_adjustment_records(conn, id_producto: int, lote: str,
                                    tipo_unidad: str, cantidad: int):
    cfg = get_pg_config()
    schema = cfg["schema"]
    tstock = cfg["table_stock"]

    pid = int(id_producto)
    cantidad = int(cantidad)
    tipo_unidad = str(tipo_unidad).upper().strip()
    lote = str(lote or LOTE_AJUSTE_DEFAULT).strip() or LOTE_AJUSTE_DEFAULT

    if cantidad <= 0:
        return []

    inserted = []
    next_serie = get_next_nro_serie(conn, pid)

    with conn.cursor() as cur:
        if tipo_unidad == "PALLET":
            for i in range(cantidad):
                nro_serie = next_serie + i
                cur.execute(f"""
                    INSERT INTO {schema}.{tstock}
                        (id_producto, lote, tipo_unidad, nro_serie, packs)
                    VALUES (%s, %s, 'PALLET', %s, NULL);
                """, (pid, lote, nro_serie))
                inserted.append(str(nro_serie))

        elif tipo_unidad == "PACKS":
            cur.execute(f"""
                INSERT INTO {schema}.{tstock}
                    (id_producto, lote, tipo_unidad, nro_serie, packs)
                VALUES (%s, %s, 'PACKS', %s, %s);
            """, (pid, lote, next_serie, cantidad))
            inserted.append(f"{next_serie} (+{cantidad} packs)")

        else:
            raise ValueError("Tipo de unidad inválido para alta de ajuste.")

    return inserted


def delete_from_stock_iterative(conn, id_producto: int, lote: str, tipo_unidad: str, cantidad: int):
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


def delete_from_stock_general_iterative(conn, id_producto: int, tipo_unidad: str, cantidad: int):
    cfg = get_pg_config()
    schema = cfg["schema"]
    tstock = cfg["table_stock"]

    tipo_unidad = str(tipo_unidad).upper().strip()
    cantidad = int(cantidad)
    afectadas = []
    bajas_por_lote = {}

    def add_lote_qty(lote_value, qty):
        lote_txt = str(lote_value or "SIN LOTE").strip() or "SIN LOTE"
        bajas_por_lote[lote_txt] = bajas_por_lote.get(lote_txt, 0) + int(qty)

    with conn.cursor() as cur:
        if tipo_unidad == "PALLET":
            cur.execute(f"""
                SELECT ctid::text, nro_serie, COALESCE(lote::text, 'SIN LOTE')
                FROM {schema}.{tstock}
                WHERE id_producto = %s
                  AND UPPER(BTRIM(COALESCE(tipo_unidad, ''))) IN ('PALLET', 'PALLETS')
                ORDER BY lote ASC, nro_serie ASC
                LIMIT %s
                FOR UPDATE;
            """, (int(id_producto), int(cantidad)))

            rows = cur.fetchall()

            if len(rows) < cantidad:
                raise ValueError(f"No hay pallets suficientes para ajustar. Disponibles: {len(rows)}")

            for ctid_txt, nro_serie, lote_val in rows:
                cur.execute(f"""
                    DELETE FROM {schema}.{tstock}
                    WHERE ctid = %s::tid;
                """, (ctid_txt,))

                if cur.rowcount != 1:
                    raise ValueError(f"No se pudo eliminar correctamente el pallet serie {nro_serie}.")

                afectadas.append(str(nro_serie))
                add_lote_qty(lote_val, 1)

        elif tipo_unidad == "PACKS":
            cur.execute(f"""
                SELECT ctid::text, nro_serie, COALESCE(lote::text, 'SIN LOTE'), COALESCE(packs, 0)
                FROM {schema}.{tstock}
                WHERE id_producto = %s
                  AND UPPER(BTRIM(COALESCE(tipo_unidad, ''))) = 'PACKS'
                  AND COALESCE(packs, 0) > 0
                ORDER BY lote ASC, nro_serie ASC
                FOR UPDATE;
            """, (int(id_producto),))

            rows = cur.fetchall()
            remaining = int(cantidad)
            disponibles = sum(int(r[3] or 0) for r in rows)

            if disponibles < remaining:
                raise ValueError(f"No hay packs suficientes para ajustar. Disponibles: {disponibles}")

            for ctid_txt, nro_serie, lote_val, packs_val in rows:
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
                    add_lote_qty(lote_val, packs_val)

                else:
                    cur.execute(f"""
                        UPDATE {schema}.{tstock}
                        SET packs = packs - %s
                        WHERE ctid = %s::tid;
                    """, (remaining, ctid_txt))

                    if cur.rowcount != 1:
                        raise ValueError(f"No se pudo actualizar correctamente el registro packs serie {nro_serie}.")

                    afectadas.append(f"{nro_serie} (-{remaining} packs)")
                    add_lote_qty(lote_val, remaining)
                    remaining = 0

            if remaining > 0:
                raise ValueError("No se pudo completar el ajuste iterativo de packs.")

        else:
            raise ValueError("Tipo de unidad inválido para ajuste.")

    return afectadas, bajas_por_lote


# =======================
#   AJUSTE EN LOTE
# =======================
def format_diff(diff_pallets: int, diff_packs: int) -> str:
    parts = []

    if diff_pallets > 0:
        parts.append(f"Pallets +{diff_pallets}")
    elif diff_pallets < 0:
        parts.append(f"Pallets {diff_pallets}")

    if diff_packs > 0:
        parts.append(f"Packs +{diff_packs}")
    elif diff_packs < 0:
        parts.append(f"Packs {diff_packs}")

    return " | ".join(parts) if parts else "Sin cambios"


def aplicar_ajuste_producto_en_transaccion(conn, cambio: dict) -> dict:
    pid = int(cambio["id_producto"])
    nuevo_pallets = int(cambio["nuevo_pallets"])
    nuevo_packs = int(cambio["nuevo_packs"])

    if nuevo_pallets < 0 or nuevo_packs < 0:
        raise ValueError(f"El producto {pid} tiene valores negativos.")

    desc, pallets_antes, packs_antes = get_product_net_stock(conn, pid)

    diff_pallets = nuevo_pallets - pallets_antes
    diff_packs = nuevo_packs - packs_antes

    bajas_ids = []

    if diff_pallets == 0 and diff_packs == 0:
        return {
            "id_producto": pid,
            "descripcion": desc,
            "pallets_antes": pallets_antes,
            "packs_antes": packs_antes,
            "pallets_despues": pallets_antes,
            "packs_despues": packs_antes,
            "diff_pallets": 0,
            "diff_packs": 0,
            "bajas_ids": [],
            "sin_cambios": True,
        }

    lote_alta = get_lote_para_alta(conn, pid)

    if diff_pallets > 0:
        insert_stock_adjustment_records(conn, pid, lote_alta, "PALLET", diff_pallets)

    elif diff_pallets < 0:
        _, bajas_por_lote = delete_from_stock_general_iterative(
            conn,
            pid,
            "PALLET",
            abs(diff_pallets)
        )

        for lote, qty in bajas_por_lote.items():
            baja_id = registrar_baja(
                conn,
                pid,
                lote,
                qty,
                "Ajuste de stock",
                f"Ajuste administrativo. Stock anterior: {pallets_antes} pallets. Stock nuevo: {nuevo_pallets} pallets.",
                tipo_unidad="PALLET"
            )
            bajas_ids.append(baja_id)

    if diff_packs > 0:
        insert_stock_adjustment_records(conn, pid, lote_alta, "PACKS", diff_packs)

    elif diff_packs < 0:
        _, bajas_por_lote = delete_from_stock_general_iterative(
            conn,
            pid,
            "PACKS",
            abs(diff_packs)
        )

        for lote, qty in bajas_por_lote.items():
            baja_id = registrar_baja(
                conn,
                pid,
                lote,
                qty,
                "Ajuste de stock",
                f"Ajuste administrativo. Stock anterior: {packs_antes} packs. Stock nuevo: {nuevo_packs} packs.",
                tipo_unidad="PACKS"
            )
            bajas_ids.append(baja_id)

    desc_final, pallets_despues, packs_despues = get_product_net_stock(conn, pid)
    upsert_sheet(conn, pid, pallets_despues, packs_despues)

    return {
        "id_producto": pid,
        "descripcion": desc_final,
        "pallets_antes": pallets_antes,
        "packs_antes": packs_antes,
        "pallets_despues": pallets_despues,
        "packs_despues": packs_despues,
        "diff_pallets": pallets_despues - pallets_antes,
        "diff_packs": packs_despues - packs_antes,
        "bajas_ids": bajas_ids,
        "sin_cambios": False,
    }


def set_stock_lote(conn, cambios: list[dict]) -> list[dict]:
    """
    Aplica todos los ajustes juntos.
    Si uno falla, se hace rollback completo.
    """
    if not cambios:
        return []

    prev_autocommit = conn.autocommit
    resultados = []

    try:
        conn.autocommit = False

        for cambio in cambios:
            res = aplicar_ajuste_producto_en_transaccion(conn, cambio)
            if not res.get("sin_cambios"):
                resultados.append(res)

        conn.commit()

    except Exception:
        conn.rollback()
        raise

    finally:
        conn.autocommit = prev_autocommit

    return resultados


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
#   DIALOGO CONTRASEÑA
# =======================
def pedir_contrasena(parent) -> bool:
    dialog = tk.Toplevel(parent)
    dialog.title("Acceso restringido")
    dialog.resizable(False, False)
    dialog.grab_set()
    dialog.focus_set()

    parent.update_idletasks()
    px = parent.winfo_x() + parent.winfo_width() // 2
    py = parent.winfo_y() + parent.winfo_height() // 2
    dialog.geometry(f"380x200+{px - 190}+{py - 100}")

    resultado = {"ok": False}

    frame = tb.Frame(dialog, padding=24)
    frame.pack(fill="both", expand=True)

    tb.Label(
        frame,
        text="🔒  Zona Administrativa",
        font=("Segoe UI", 13, "bold")
    ).pack(pady=(0, 6))

    tb.Label(
        frame,
        text="Ingresá la contraseña para continuar:",
        font=("Segoe UI", 10)
    ).pack(pady=(0, 10))

    pwd_var = tk.StringVar()
    pwd_entry = tb.Entry(frame, textvariable=pwd_var, show="*", width=28, font=("Segoe UI", 11))
    pwd_entry.pack(pady=(0, 6))
    pwd_entry.focus_set()

    error_label = tb.Label(frame, text="", foreground="red", font=("Segoe UI", 9))
    error_label.pack()

    def confirmar(event=None):
        if pwd_var.get() == PASSWORD_ADMIN:
            resultado["ok"] = True
            dialog.destroy()
        else:
            error_label.configure(text="Contraseña incorrecta. Intentá de nuevo.")
            pwd_var.set("")
            pwd_entry.focus_set()

    def cancelar():
        dialog.destroy()

    btn_frame = tb.Frame(frame)
    btn_frame.pack(pady=(10, 0))

    tb.Button(
        btn_frame,
        text="Ingresar",
        bootstyle=SUCCESS,
        width=12,
        command=confirmar
    ).pack(side="left", padx=6)

    tb.Button(
        btn_frame,
        text="Cancelar",
        bootstyle=SECONDARY,
        width=12,
        command=cancelar
    ).pack(side="left", padx=6)

    pwd_entry.bind("<Return>", confirmar)

    dialog.wait_window()
    return resultado["ok"]


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
    # TABS
    # =========================================================
    tab_bajas = tb.Frame(notebook)
    tab_stock = tb.Frame(notebook, padding=18)
    tab_editor = tb.Frame(notebook, padding=18)

    notebook.add(tab_bajas, text="Bajas")
    notebook.add(tab_stock, text="Stock actual")
    notebook.add(tab_editor, text="🔒 Editor de stock")

    # =========================================================
    # TAB BAJAS CON SCROLL
    # =========================================================
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
    # PESTAÑA STOCK ACTUAL
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
    # PESTAÑA EDITOR DE STOCK
    # =========================================================
    editor_unlocked = {"value": False}

    lock_frame = tb.Frame(tab_editor)
    lock_frame.place(relx=0, rely=0, relwidth=1, relheight=1)

    tb.Label(
        lock_frame,
        text="🔒",
        font=("Segoe UI", 48)
    ).pack(pady=(80, 10))

    tb.Label(
        lock_frame,
        text="Esta sección es de uso administrativo.",
        font=("Segoe UI", 13)
    ).pack()

    tb.Label(
        lock_frame,
        text="Hacé clic en el botón para ingresar con contraseña.",
        font=("Segoe UI", 10),
        foreground="gray"
    ).pack(pady=(4, 20))

    editor_frame = tb.Frame(tab_editor)

    editor_header = tb.Frame(editor_frame)
    editor_header.pack(fill="x", pady=(0, 10))

    tb.Label(
        editor_header,
        text="Editor de stock",
        font=("Segoe UI", 18, "bold")
    ).pack(side="left")

    btn_enviar_cambios = tb.Button(
        editor_header,
        text="ENVIAR TODOS LOS CAMBIOS",
        bootstyle=DANGER,
        width=28
    )
    btn_enviar_cambios.pack(side="right", padx=(6, 0))

    btn_refresh_editor = tb.Button(
        editor_header,
        text="Actualizar",
        bootstyle=INFO,
        width=14
    )
    btn_refresh_editor.pack(side="right", padx=(6, 0))

    btn_descartar_cambios = tb.Button(
        editor_header,
        text="Descartar cambios",
        bootstyle=SECONDARY,
        width=18
    )
    btn_descartar_cambios.pack(side="right", padx=(6, 0))

    btn_lock_editor = tb.Button(
        editor_header,
        text="🔒 Bloquear",
        bootstyle=SECONDARY,
        width=14
    )
    btn_lock_editor.pack(side="right")

    tb.Label(
        editor_frame,
        text=(
            "Editá directamente en la tabla las columnas 'Pallets nuevos' y 'Packs nuevos' "
            "con doble clic. Luego enviá todos los cambios juntos."
        ),
        font=("Segoe UI", 9),
        foreground="gray"
    ).pack(anchor="w", pady=(0, 8))

    editor_card = ttk.LabelFrame(editor_frame, text="Productos y stock", padding=10)
    editor_card.pack(fill="both", expand=True)

    editor_table_frame = tb.Frame(editor_card)
    editor_table_frame.pack(fill="both", expand=True)

    editor_tree = ttk.Treeview(
        editor_table_frame,
        columns=(
            "codigo",
            "descripcion",
            "pallets_actuales",
            "packs_actuales",
            "pallets_nuevos",
            "packs_nuevos",
            "diferencia"
        ),
        show="headings",
        height=16,
        selectmode="browse"
    )

    editor_tree.heading("codigo", text="Código")
    editor_tree.heading("descripcion", text="Descripción")
    editor_tree.heading("pallets_actuales", text="Pallets actuales")
    editor_tree.heading("packs_actuales", text="Packs actuales")
    editor_tree.heading("pallets_nuevos", text="Pallets nuevos")
    editor_tree.heading("packs_nuevos", text="Packs nuevos")
    editor_tree.heading("diferencia", text="Diferencia")

    editor_tree.column("codigo", width=80, anchor="center")
    editor_tree.column("descripcion", width=420, anchor="w")
    editor_tree.column("pallets_actuales", width=120, anchor="center")
    editor_tree.column("packs_actuales", width=110, anchor="center")
    editor_tree.column("pallets_nuevos", width=120, anchor="center")
    editor_tree.column("packs_nuevos", width=110, anchor="center")
    editor_tree.column("diferencia", width=220, anchor="center")

    editor_scroll = ttk.Scrollbar(editor_table_frame, orient="vertical", command=editor_tree.yview)
    editor_tree.configure(yscrollcommand=editor_scroll.set)

    editor_tree.pack(side="left", fill="both", expand=True)
    editor_scroll.pack(side="right", fill="y")

    edit_panel = ttk.LabelFrame(editor_frame, text="Estado de los ajustes", padding=12)
    edit_panel.pack(fill="x", pady=(10, 0))

    edit_status_var = tk.StringVar(value="Sin cambios pendientes.")
    edit_status_label = tb.Label(edit_panel, textvariable=edit_status_var, font=("Segoe UI", 10))
    edit_status_label.pack(anchor="w")

    # =========================================================
    # FUNCIONES UI STOCK / EDITOR
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

    def refresh_editor_tree():
        for item in editor_tree.get_children():
            editor_tree.delete(item)

        try:
            rows = get_stock_summary_all_products(conn)

            if not rows:
                editor_tree.insert("", "end", values=("-", "Sin productos", 0, 0, 0, 0, "Sin cambios"))
                return

            for pid, desc, pallets, packs in rows:
                tag = "con_stock" if (pallets > 0 or packs > 0) else "sin_stock"
                editor_tree.insert(
                    "",
                    "end",
                    values=(pid, desc, pallets, packs, pallets, packs, "Sin cambios"),
                    tags=(tag,)
                )

            editor_tree.tag_configure("con_stock", background="#e8f5e9")
            editor_tree.tag_configure("sin_stock", background="#ffffff")
            edit_status_var.set("🟢 Stock cargado. Editá con doble clic las columnas de nuevos valores.")
            edit_status_label.configure(foreground="green")

        except Exception as e:
            editor_tree.insert("", "end", values=("-", f"Error: {e}", "-", "-", "-", "-", "-"))
            edit_status_var.set(f"❌ Error al cargar editor: {e}")
            edit_status_label.configure(foreground="red")

    def safe_int(value, field_name="valor"):
        value = str(value).strip()

        if value == "":
            raise ValueError(f"El campo {field_name} no puede estar vacío.")

        if not value.isdigit():
            raise ValueError(f"El campo {field_name} debe ser un número entero mayor o igual a 0.")

        return int(value)

    def recalcular_diferencia_item(item_id):
        vals = list(editor_tree.item(item_id, "values"))

        try:
            pallets_actuales = int(vals[2])
            packs_actuales = int(vals[3])
            pallets_nuevos = safe_int(vals[4], "Pallets nuevos")
            packs_nuevos = safe_int(vals[5], "Packs nuevos")

            diff_p = pallets_nuevos - pallets_actuales
            diff_pk = packs_nuevos - packs_actuales

            vals[6] = format_diff(diff_p, diff_pk)

            if diff_p != 0 or diff_pk != 0:
                editor_tree.item(item_id, values=vals, tags=("modificado",))
            else:
                tag = "con_stock" if (pallets_actuales > 0 or packs_actuales > 0) else "sin_stock"
                editor_tree.item(item_id, values=vals, tags=(tag,))

            editor_tree.tag_configure("modificado", background="#fff3cd")
            editor_tree.tag_configure("con_stock", background="#e8f5e9")
            editor_tree.tag_configure("sin_stock", background="#ffffff")

            cambios = get_editor_changes(validar=False)
            if cambios:
                edit_status_var.set(f"✏️ Hay {len(cambios)} cambio(s) pendiente(s).")
                edit_status_label.configure(foreground="#b7791f")
            else:
                edit_status_var.set("Sin cambios pendientes.")
                edit_status_label.configure(foreground="gray")

        except Exception as e:
            vals[6] = f"Error: {e}"
            editor_tree.item(item_id, values=vals, tags=("error",))
            editor_tree.tag_configure("error", background="#f8d7da")

    def editar_celda_editor(event):
        region = editor_tree.identify("region", event.x, event.y)

        if region != "cell":
            return

        row_id = editor_tree.identify_row(event.y)
        col_id = editor_tree.identify_column(event.x)

        # Columnas editables:
        # #5 = pallets_nuevos
        # #6 = packs_nuevos
        if not row_id or col_id not in ("#5", "#6"):
            return

        col_index = int(col_id.replace("#", "")) - 1
        bbox = editor_tree.bbox(row_id, col_id)

        if not bbox:
            return

        x, y, width, height = bbox
        current_values = list(editor_tree.item(row_id, "values"))
        current_value = current_values[col_index]

        editor = tb.Entry(editor_tree, width=max(8, int(width / 10)))
        editor.insert(0, str(current_value))
        editor.select_range(0, "end")
        editor.place(x=x, y=y, width=width, height=height)
        editor.focus_set()

        def save_edit(event=None):
            new_value = editor.get().strip()

            try:
                safe_int(new_value, "stock nuevo")
                vals = list(editor_tree.item(row_id, "values"))
                vals[col_index] = new_value
                editor_tree.item(row_id, values=vals)
                recalcular_diferencia_item(row_id)

            except Exception as e:
                messagebox.showerror("Valor inválido", str(e), parent=root)

            finally:
                try:
                    editor.destroy()
                except Exception:
                    pass

        editor.bind("<Return>", save_edit)
        editor.bind("<FocusOut>", save_edit)
        editor.bind("<Escape>", lambda e: editor.destroy())

    def get_editor_changes(validar=True):
        cambios = []

        for item in editor_tree.get_children():
            vals = list(editor_tree.item(item, "values"))

            if len(vals) < 7:
                continue

            try:
                pid = int(vals[0])
                desc = str(vals[1])
                pallets_actuales = int(vals[2])
                packs_actuales = int(vals[3])
                pallets_nuevos = safe_int(vals[4], f"Pallets nuevos del producto {pid}")
                packs_nuevos = safe_int(vals[5], f"Packs nuevos del producto {pid}")

                if pallets_nuevos != pallets_actuales or packs_nuevos != packs_actuales:
                    cambios.append({
                        "id_producto": pid,
                        "descripcion": desc,
                        "pallets_actuales": pallets_actuales,
                        "packs_actuales": packs_actuales,
                        "nuevo_pallets": pallets_nuevos,
                        "nuevo_packs": packs_nuevos,
                        "diff_pallets": pallets_nuevos - pallets_actuales,
                        "diff_packs": packs_nuevos - packs_actuales,
                    })

            except Exception:
                if validar:
                    raise
                continue

        return cambios

    def build_confirmacion_cambios(cambios):
        lineas = [
            f"Se van a aplicar {len(cambios)} ajuste(s) de stock.",
            "",
            "Resumen:",
            "",
        ]

        for c in cambios[:15]:
            lineas.append(
                f"[{c['id_producto']}] {c['descripcion']}\n"
                f"  Actual: {c['pallets_actuales']} pallets / {c['packs_actuales']} packs\n"
                f"  Nuevo:  {c['nuevo_pallets']} pallets / {c['nuevo_packs']} packs\n"
                f"  Dif.:   {format_diff(c['diff_pallets'], c['diff_packs'])}\n"
            )

        if len(cambios) > 15:
            lineas.append(f"... y {len(cambios) - 15} producto(s) más.")

        lineas.append("")
        lineas.append("Esta acción aplicará todos los cambios juntos y enviará un mail informando las diferencias.")
        lineas.append("¿Confirmás continuar?")

        return "\n".join(lineas)

    def enviar_todos_los_cambios():
        try:
            cambios = get_editor_changes(validar=True)

        except Exception as e:
            messagebox.showerror("Valores inválidos", str(e), parent=root)
            return

        if not cambios:
            messagebox.showinfo("Sin cambios", "No hay cambios pendientes para enviar.", parent=root)
            return

        confirm = messagebox.askyesno(
            "Confirmar envío de ajustes",
            build_confirmacion_cambios(cambios),
            parent=root
        )

        if not confirm:
            edit_status_var.set("Operación cancelada.")
            edit_status_label.configure(foreground="gray")
            return

        try:
            btn_enviar_cambios.configure(state="disabled", text="Procesando...")
            btn_refresh_editor.configure(state="disabled")
            btn_descartar_cambios.configure(state="disabled")
            root.update_idletasks()

            resultados = set_stock_lote(conn, cambios)

            if not resultados:
                edit_status_var.set("ℹ️ No hubo cambios para aplicar.")
                edit_status_label.configure(foreground="gray")
                return

            for cambio in resultados:
                _sync_google_stock_async(
                    cambio["id_producto"],
                    cambio["descripcion"],
                    cambio["pallets_despues"],
                    cambio["packs_despues"]
                )

            send_stock_changes_email_async(resultados)

            edit_status_var.set(
                f"✅ Se aplicaron {len(resultados)} ajuste(s). "
                f"Se está sincronizando Google Sheet y enviando el mail a {EMAIL_AJUSTES_TO}."
            )
            edit_status_label.configure(foreground="green")

            set_status(
                f"✅ Ajustes de stock aplicados: {len(resultados)} producto(s).\n"
                f"Se está sincronizando Google Sheet y enviando el mail a {EMAIL_AJUSTES_TO}."
            )

            refresh_editor_tree()
            refresh_stock_box()
            refresh_product_combo()

        except Exception as e:
            edit_status_var.set(f"❌ Error al aplicar ajustes: {e}")
            edit_status_label.configure(foreground="red")
            messagebox.showerror("Error al aplicar ajustes", str(e), parent=root)

        finally:
            btn_enviar_cambios.configure(state="normal", text="ENVIAR TODOS LOS CAMBIOS")
            btn_refresh_editor.configure(state="normal")
            btn_descartar_cambios.configure(state="normal")

    editor_tree.bind("<Double-1>", editar_celda_editor)
    btn_enviar_cambios.configure(command=enviar_todos_los_cambios)
    btn_descartar_cambios.configure(command=refresh_editor_tree)
    btn_refresh_editor.configure(command=refresh_editor_tree)

    def bloquear_editor():
        editor_unlocked["value"] = False
        editor_frame.place_forget()
        lock_frame.place(relx=0, rely=0, relwidth=1, relheight=1)

    btn_lock_editor.configure(command=bloquear_editor)

    def desbloquear_editor():
        if pedir_contrasena(root):
            editor_unlocked["value"] = True
            lock_frame.place_forget()
            editor_frame.place(relx=0, rely=0, relwidth=1, relheight=1)
            refresh_editor_tree()
        else:
            notebook.select(0)

    btn_desbloquear = tb.Button(
        lock_frame,
        text="🔓  Ingresar contraseña",
        bootstyle=WARNING,
        width=26,
        command=desbloquear_editor
    )
    btn_desbloquear.pack()

    # =========================================================
    # FUNCIONES BAJAS
    # =========================================================
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
                baja_id,
                pid,
                desc,
                lote,
                tipo_unidad,
                cantidad,
                net_p,
                net_pk,
                series_afectadas,
                cliente_final,
                cliente_creado
            ) = baja_manual(
                conn,
                pid,
                lote,
                tipo,
                qty,
                motivo,
                obs,
                cliente
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

            elif current_index == 2:
                if not editor_unlocked["value"]:
                    desbloquear_editor()

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