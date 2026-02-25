import os
import sys
import json
from datetime import datetime
import urllib.request
import urllib.error

import ttkbootstrap as tb
from ttkbootstrap.constants import *
from tkinter import messagebox

# ----------------------------
#   POSTGRES DRIVER
# ----------------------------
try:
    import psycopg2
    from psycopg2.extras import Json
except Exception:
    psycopg2 = None
    Json = None


# =======================
#   CONFIG / PATHS
# =======================
def get_app_dir():
    if getattr(sys, "frozen", False):
        return os.path.dirname(sys.executable)
    return os.path.dirname(os.path.abspath(__file__))


APP_DIR = get_app_dir()
CACHE_FILE = os.path.join(APP_DIR, "config.json")


# =======================
#   GOOGLE SHEETS (WEBHOOK)
# =======================
SHEETS_WEBAPP_URL = "https://script.google.com/macros/s/AKfycbz3HnhMu8ylXKtiEVvcsIRc_VKJzxUQHotKDOHT74QgTgLIVbJPPiX3eJBly368Ad4/exec"
SHEETS_API_KEY = "TALCA-QR-2026"  # Debe coincidir con API_KEY en Apps Script


# =======================
#   POSTGRES DEFAULTS
# =======================
DEFAULT_PG = {
    "host": os.getenv("TALCA_PG_HOST", "localhost"),
    "port": int(os.getenv("TALCA_PG_PORT", "5432")),
    "dbname": os.getenv("TALCA_PG_DB", "postgres"),
    "user": os.getenv("TALCA_PG_USER", "postgres"),
    "password": os.getenv("TALCA_PG_PASS", ""),
    "client_encoding": os.getenv("TALCA_PG_ENCODING", ""),  # opcional
    "schema": "stock",
    "table_products": "productos",
    "table_pp": "stock_pp",
    "table_outbox": "sheets_outbox",
    "table_salidas": "salidas_qr",
}


# =======================
#   CACHE
# =======================
def load_cache():
    if not os.path.exists(CACHE_FILE):
        return {}

    # Leemos como bytes (para no romper por encoding)
    raw = open(CACHE_FILE, "rb").read()

    for enc in ("utf-8", "utf-8-sig", "cp1252", "latin-1"):
        try:
            txt = raw.decode(enc)
            data = json.loads(txt)

            # Si se pudo leer, lo re-guardamos en UTF-8 para que no vuelva a pasar
            with open(CACHE_FILE, "w", encoding="utf-8") as wf:
                json.dump(data, wf, ensure_ascii=False, indent=2)

            return data

        except UnicodeDecodeError:
            continue

    raise RuntimeError("No pude decodificar config.json. Guardalo como UTF-8.")

def get_pg_config():
    cfg = DEFAULT_PG.copy()
    cache = load_cache()
    if isinstance(cache.get("pg"), dict):
        for k, v in cache["pg"].items():
            if v is not None and v != "":
                cfg[k] = v

    try:
        cfg["port"] = int(cfg["port"])
    except Exception:
        cfg["port"] = 5432
    return cfg


# =======================
#   NORMALIZACIONES
# =======================
def normalize_id_value(x) -> str:
    if x is None:
        return ""
    s = str(x).strip()
    if s == "" or s.lower() == "nan":
        return ""
    try:
        f = float(s)
        if f.is_integer():
            return str(int(f))
    except Exception:
        pass
    return s


def normalize_date_iso(s: str) -> str:
    if not s:
        return ""
    s = str(s).strip()
    if "/" in s:
        try:
            d = datetime.strptime(s, "%d/%m/%y").date()
            return d.isoformat()
        except Exception:
            return s
    return s


# =======================
#   POSTGRES
# =======================
def pg_connect():
    """
    Conecta a PostgreSQL usando config.json (sección pg)
    Aplica client_encoding si está definido (ej: WIN1252)
    """
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
        try:
            conn.set_client_encoding(enc)
        except Exception as e:
            raise RuntimeError(f"Conectó a PG pero falló client_encoding='{enc}': {e}")

    return conn


def init_tables(conn):
    """
    - Crea schema si no existe
    - Crea outbox para Sheets (misma usada por escaner.py)
    - Crea tabla de salidas por QR (marca de baja)
    """
    cfg = get_pg_config()
    schema = cfg["schema"]
    outbox = cfg["table_outbox"]
    salidas = cfg["table_salidas"]

    with conn.cursor() as cur:
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")

        # outbox (cola) para reintentos de Sheets
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {schema}.{outbox} (
                id BIGSERIAL PRIMARY KEY,
                payload JSONB NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT now()
            );
        """)
        cur.execute(f"CREATE INDEX IF NOT EXISTS idx_{outbox}_created_at ON {schema}.{outbox}(created_at DESC);")

        # tabla de salidas (cada QR dado de baja queda registrado acá)
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {schema}.{salidas} (
                id BIGSERIAL PRIMARY KEY,
                created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                id_producto INT NOT NULL,
                lote TEXT NOT NULL,
                nro_serie INT NOT NULL,
                unit_type TEXT NOT NULL,         -- 'pallet' o 'packs'
                packs_qty INT NOT NULL DEFAULT 0,
                stock_pp_id BIGINT NULL,
                raw_payload TEXT NULL,
                CONSTRAINT uq_{salidas}_qr UNIQUE (id_producto, lote, nro_serie)
            );
        """)
        cur.execute(f"CREATE INDEX IF NOT EXISTS idx_{salidas}_prod_lote ON {schema}.{salidas}(id_producto, lote);")


# =======================
#   SHEETS OUTBOX
# =======================
def outbox_count(conn):
    cfg = get_pg_config()
    schema = cfg["schema"]
    outbox = cfg["table_outbox"]
    with conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {schema}.{outbox};")
        return int(cur.fetchone()[0] or 0)


def queue_outbox(conn, payload: dict):
    cfg = get_pg_config()
    schema = cfg["schema"]
    outbox = cfg["table_outbox"]
    with conn.cursor() as cur:
        cur.execute(f"INSERT INTO {schema}.{outbox}(payload) VALUES (%s);", (Json(payload),))


def pop_outbox_batch(conn, limit=50):
    cfg = get_pg_config()
    schema = cfg["schema"]
    outbox = cfg["table_outbox"]
    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT id, payload
            FROM {schema}.{outbox}
            ORDER BY id ASC
            LIMIT %s;
        """, (int(limit),))
        return cur.fetchall()


def delete_outbox_id(conn, rid: int):
    cfg = get_pg_config()
    schema = cfg["schema"]
    outbox = cfg["table_outbox"]
    with conn.cursor() as cur:
        cur.execute(f"DELETE FROM {schema}.{outbox} WHERE id = %s;", (int(rid),))


def send_to_sheets(payload: dict):
    data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(
        SHEETS_WEBAPP_URL,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST"
    )

    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
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
        raise RuntimeError(f"HTTP {e.code}: {body or e.reason}")


def flush_outbox(conn):
    rows = pop_outbox_batch(conn, limit=50)
    sent = 0
    for rid, payload in rows:
        res = send_to_sheets(payload)
        if isinstance(res, dict) and res.get("ok") is True:
            delete_outbox_id(conn, rid)
            sent += 1
        else:
            break
    return sent


# =======================
#   PARSEO QR
# =======================
def parse_qr_payload(raw: str) -> dict:
    """
    Formato:
    NS=000001|PRD=12|DSC=Descripcion...|LOT=090226|FEC=2026-02-09|VTO=2026-08-09
    """
    raw = raw.strip()

    if "|" in raw and "=" in raw:
        parts = raw.split("|")
        data = {}
        for p in parts:
            if "=" in p:
                k, v = p.split("=", 1)
                data[k.strip()] = v.strip()

        required = ["NS", "PRD", "DSC", "LOT", "FEC", "VTO"]
        missing = [k for k in required if k not in data or not data[k]]
        if missing:
            raise ValueError(f"QR inválido, faltan campos: {', '.join(missing)}")

        return {
            "descripcion": data["DSC"],
            "nro_serie": int(data["NS"]),
            "id_producto": int(normalize_id_value(data["PRD"])),
            "lote": str(data["LOT"]).strip(),
            "creacion": normalize_date_iso(data["FEC"]),
            "vencimiento": normalize_date_iso(data["VTO"]),
        }

    raise ValueError("QR inválido: formato no reconocido.")


# =======================
#   LÓGICA DE STOCK (INGRESO - SALIDA)
# =======================
def find_stock_pp_row_for_serial(conn, id_producto: int, lote: str, nro_serie: int):
    """
    Busca el movimiento de ingreso (stock_pp) donde cayó ese nro_serie
    """
    cfg = get_pg_config()
    schema = cfg["schema"]
    tpp = cfg["table_pp"]

    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT id, serie_inicio, serie_fin, packs_fin
            FROM {schema}.{tpp}
            WHERE id_producto = %s
              AND lote = %s
              AND serie_inicio <= %s
              AND serie_fin >= %s
            ORDER BY created_at ASC
            LIMIT 1;
        """, (int(id_producto), str(lote), int(nro_serie), int(nro_serie)))
        return cur.fetchone()  # (id, serie_inicio, serie_fin, packs_fin) o None


def compute_ingresos_totals(conn, id_producto: int, lote: str):
    """
    Ingresos:
      pallets = SUM( (serie_fin-serie_inicio+1) - (packs_fin>0 ? 1 : 0) )
      packs   = SUM(packs_fin)
    """
    cfg = get_pg_config()
    schema = cfg["schema"]
    tpp = cfg["table_pp"]
    prod = cfg["table_products"]

    with conn.cursor() as cur:
        cur.execute(f"SELECT descripcion FROM {schema}.{prod} WHERE id_producto = %s;", (int(id_producto),))
        row = cur.fetchone()
        desc = row[0] if row else ""

        cur.execute(f"""
            SELECT
                COALESCE(SUM((serie_fin - serie_inicio + 1) - CASE WHEN packs_fin > 0 THEN 1 ELSE 0 END), 0) AS pallets,
                COALESCE(SUM(packs_fin), 0) AS packs
            FROM {schema}.{tpp}
            WHERE id_producto = %s AND lote = %s;
        """, (int(id_producto), str(lote)))
        pallets, packs = cur.fetchone()

    return int(pallets), int(packs), str(desc).strip()


def compute_salidas_totals(conn, id_producto: int, lote: str):
    """
    Salidas:
      pallets = COUNT(unit_type='pallet')
      packs   = SUM(packs_qty) para unit_type='packs'
    """
    cfg = get_pg_config()
    schema = cfg["schema"]
    salidas = cfg["table_salidas"]

    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT
                COALESCE(SUM(CASE WHEN unit_type = 'pallet' THEN 1 ELSE 0 END), 0) AS pallets_out,
                COALESCE(SUM(CASE WHEN unit_type = 'packs' THEN packs_qty ELSE 0 END), 0) AS packs_out
            FROM {schema}.{salidas}
            WHERE id_producto = %s AND lote = %s;
        """, (int(id_producto), str(lote)))
        pallets_out, packs_out = cur.fetchone()

    return int(pallets_out), int(packs_out)


def compute_net_stock(conn, id_producto: int, lote: str):
    in_p, in_pk, desc = compute_ingresos_totals(conn, id_producto, lote)
    out_p, out_pk = compute_salidas_totals(conn, id_producto, lote)
    net_p = in_p - out_p
    net_pk = in_pk - out_pk
    return net_p, net_pk, desc


def salida_already_done(conn, id_producto: int, lote: str, nro_serie: int) -> bool:
    cfg = get_pg_config()
    schema = cfg["schema"]
    salidas = cfg["table_salidas"]

    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT 1
            FROM {schema}.{salidas}
            WHERE id_producto = %s AND lote = %s AND nro_serie = %s
            LIMIT 1;
        """, (int(id_producto), str(lote), int(nro_serie)))
        return cur.fetchone() is not None


def insert_salida(conn, id_producto: int, lote: str, nro_serie: int, unit_type: str, packs_qty: int, stock_pp_id: int, raw_payload: str):
    cfg = get_pg_config()
    schema = cfg["schema"]
    salidas = cfg["table_salidas"]

    with conn.cursor() as cur:
        cur.execute(f"""
            INSERT INTO {schema}.{salidas}(id_producto, lote, nro_serie, unit_type, packs_qty, stock_pp_id, raw_payload)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            RETURNING id, created_at;
        """, (int(id_producto), str(lote), int(nro_serie), str(unit_type), int(packs_qty), stock_pp_id, raw_payload))
        return cur.fetchone()  # (id, created_at)


def build_payload_for_product_lote_net(conn, id_producto: int, lote: str):
    net_p, net_pk, desc = compute_net_stock(conn, id_producto, lote)
    return {
        "api_key": SHEETS_API_KEY,
        "type": "scan_pp",  # mantenemos el mismo tipo para no tocar Apps Script
        "timestamp": datetime.now().isoformat(timespec="seconds"),
        "stock": {
            "id_producto": int(id_producto),
            "descripcion": desc,
            "lote": str(lote),
            "stock_pallets": int(net_p),
            "stock_packs": int(net_pk),
        }
    }


# =======================
#   UI APP
# =======================
def main():
    # Conexión PG
    try:
        conn_pg = pg_connect()
        init_tables(conn_pg)
    except Exception as e:
        messagebox.showerror(
            "PostgreSQL",
            "No pude conectar a PostgreSQL.\n\n"
            f"Error: {e}\n\n"
            "Tip: revisá host/puerto/db/user/pass en config.json (sección pg).\n"
            "Si usás client_encoding, asegurate que sea válido (ej: WIN1252)."
        )
        return

    root = tb.Window(themename="minty")
    root.title("Salida / Baja por QR – Talca (PostgreSQL)")
    root.geometry("920x560")

    tb.Label(root, text="Salida / Baja por QR", font=("Segoe UI", 20, "bold")).pack(pady=14)
    tb.Label(
        root,
        text="Escaneá 1 QR y presioná Enter.\n"
             "El sistema valida que exista en ingreso y lo descuenta del stock.\n"
             "Si ya estaba dado de baja, lo bloquea.",
        font=("Segoe UI", 10)
    ).pack(pady=6)

    scan_var = tb.StringVar()
    entry_scan = tb.Entry(root, textvariable=scan_var, width=95, font=("Segoe UI", 14))
    entry_scan.pack(pady=12)
    entry_scan.focus_set()

    status_var = tb.StringVar(value="🟢 Listo: escaneá un QR y Enter.")
    tb.Label(root, textvariable=status_var, font=("Segoe UI", 11), justify="left").pack(pady=10)

    sheets_var = tb.StringVar(value=f"Sheets pendientes (outbox): {outbox_count(conn_pg)}")
    tb.Label(root, textvariable=sheets_var, font=("Segoe UI", 10)).pack(pady=4)

    def reset_input():
        scan_var.set("")
        entry_scan.focus_set()

    def on_scan_return(event=None):
        raw = scan_var.get().strip()
        if not raw:
            return
        reset_input()

        try:
            data = parse_qr_payload(raw)
            pid = int(data["id_producto"])
            lote = str(data["lote"]).strip()
            serie = int(data["nro_serie"])

            # 1) validar que existe en ingreso (stock_pp)
            pp_row = find_stock_pp_row_for_serial(conn_pg, pid, lote, serie)
            if not pp_row:
                raise ValueError("Este QR NO está registrado en ingreso (stock_pp). No se puede dar de baja.")

            stock_pp_id, serie_inicio, serie_fin, packs_fin = pp_row
            packs_fin = int(packs_fin or 0)

            # 2) validar que no esté ya dado de baja
            if salida_already_done(conn_pg, pid, lote, serie):
                raise ValueError("Este QR ya fue dado de baja anteriormente (salida duplicada).")

            # 3) determinar unidad a descontar (pallet o packs)
            if packs_fin > 0 and int(serie) == int(serie_fin):
                unit_type = "packs"
                packs_qty = packs_fin
            else:
                unit_type = "pallet"
                packs_qty = 0

            # 4) validar stock suficiente (neto)
            net_p_before, net_pk_before, desc = compute_net_stock(conn_pg, pid, lote)

            if unit_type == "pallet":
                if net_p_before < 1:
                    raise ValueError(f"No hay pallets disponibles para dar de baja. Stock pallets actual: {net_p_before}")
            else:
                if net_pk_before < packs_qty:
                    raise ValueError(f"No hay packs suficientes para dar de baja. Stock packs actual: {net_pk_before} | Requiere: {packs_qty}")

            # 5) insertar salida
            mov_id, created_at = insert_salida(
                conn_pg,
                pid, lote, serie,
                unit_type, packs_qty,
                int(stock_pp_id),
                raw_payload=raw
            )

            # 6) calcular stock neto después
            net_p_after, net_pk_after, _ = compute_net_stock(conn_pg, pid, lote)

            # 7) Enviar update a Sheets (con outbox)
            try:
                payload = build_payload_for_product_lote_net(conn_pg, pid, lote)
                queue_outbox(conn_pg, payload)
                sent = flush_outbox(conn_pg)
                pending = outbox_count(conn_pg)
                sheets_var.set(f"✅ Enviado(s): {sent} | Pendientes (outbox): {pending}")
            except Exception as e:
                pending = outbox_count(conn_pg)
                sheets_var.set(f"⚠️ No se pudo enviar a Sheets: {e} | Pendientes (outbox): {pending}")

            root.bell()

            if unit_type == "pallet":
                baja_txt = "1 pallet"
            else:
                baja_txt = f"{packs_qty} packs (parcial)"

            status_var.set(
                f"✅ Baja registrada\n"
                f"ID salida: {mov_id} | {created_at}\n"
                f"Producto: {pid} | {desc}\n"
                f"Lote: {lote} | Serie: {serie}\n"
                f"Se descontó: {baja_txt}\n\n"
                f"📦 Stock neto pallets: {net_p_after}\n"
                f"📦 Stock neto packs: {net_pk_after}"
            )

        except Exception as e:
            status_var.set(f"❌ ERROR: {e}")
            root.bell()

    def retry_flush():
        try:
            sent = flush_outbox(conn_pg)
            pending = outbox_count(conn_pg)
            sheets_var.set(f"✅ Enviado(s): {sent} | Pendientes (outbox): {pending}")
            messagebox.showinfo("Outbox Sheets", f"Enviados: {sent}\nPendientes: {pending}")
        except Exception as e:
            messagebox.showerror("Outbox Sheets", f"Error:\n{e}")

    btn_frame = tb.Frame(root)
    btn_frame.pack(pady=10)
    tb.Button(btn_frame, text="Reintentar envío a Sheets (outbox)", bootstyle=WARNING, command=retry_flush).pack(side="left", padx=6)
    tb.Button(btn_frame, text="Limpiar", bootstyle=SECONDARY, command=lambda: status_var.set("🟢 Listo: escaneá un QR y Enter.")).pack(side="left", padx=6)

    entry_scan.bind("<Return>", on_scan_return)

    def on_close():
        try:
            conn_pg.close()
        except Exception:
            pass
        root.destroy()

    root.protocol("WM_DELETE_WINDOW", on_close)
    root.mainloop()


if __name__ == "__main__":
    main()