import os
import sys
import json
import uuid
from datetime import datetime
import threading
import urllib.request
import urllib.error

import ttkbootstrap as tb
from ttkbootstrap.constants import *

# =======================
# GOOGLE SHEET WEBAPP
# =======================
SHEETS_WEBAPP_URL = "https://script.google.com/macros/s/AKfycbyBxCyZMyEeFi_fdsRu-SjnZHktTH4XNF3APKaJJsnqoesgTKjZ8sSqF_O6NDwXa77s9Q/exec"
SHEETS_API_KEY = "TALCA-QR-2026"

try:
    import psycopg2
except Exception:
    psycopg2 = None


# =======================
# PATHS / CONFIG
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
}

MAX_AUTOFILL = 5000


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
# GOOGLE SHEET SYNC
# =======================
def _looks_like_unknown_action(res) -> bool:
    try:
        err = str(res.get("error", "")).lower()
        return "unknown action" in err
    except Exception:
        return False


def _post_json_to_webapp(payload: dict, timeout: int = 30) -> dict:
    url, _ = get_sheet_settings()

    data = json.dumps(payload, ensure_ascii=False).encode("utf-8")

    req = urllib.request.Request(
        url,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST"
    )

    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            txt = resp.read().decode("utf-8", errors="ignore")

            try:
                return json.loads(txt)
            except Exception:
                return {
                    "ok": False,
                    "raw": txt
                }

    except urllib.error.HTTPError as e:
        try:
            body = e.read().decode("utf-8", errors="ignore")
        except Exception:
            body = ""

        return {
            "ok": False,
            "http_status": getattr(e, "code", None),
            "error": str(e),
            "raw": body
        }

    except urllib.error.URLError as e:
        return {
            "ok": False,
            "error": f"URLError: {e}"
        }

    except Exception as e:
        return {
            "ok": False,
            "error": str(e)
        }


def send_bulk_to_sheet(rows, chunk_size: int = 500):
    _, api_key = get_sheet_settings()

    if rows is None:
        rows = []

    total = len(rows)
    snapshot_id = str(uuid.uuid4())
    wrote_total = 0
    blocks = 0

    if total == 0:
        payload = {
            "api_key": api_key,
            "action": "bulk_snapshot_pp",
            "type": "bulk_snapshot_pp",
            "snapshot_id": snapshot_id,
            "block_index": 0,
            "is_first_block": True,
            "is_last_block": True,
            "rows": []
        }

        return _post_json_to_webapp(payload, timeout=30)

    for start in range(0, total, chunk_size):
        chunk = rows[start:start + chunk_size]
        block_index = start // chunk_size
        blocks += 1

        payload = {
            "api_key": api_key,
            "action": "bulk_snapshot_pp",
            "type": "bulk_snapshot_pp",
            "snapshot_id": snapshot_id,
            "block_index": block_index,
            "is_first_block": start == 0,
            "is_last_block": start + chunk_size >= total,
            "rows": chunk
        }

        res = _post_json_to_webapp(payload, timeout=90)

        if not (isinstance(res, dict) and res.get("ok") is True):
            if isinstance(res, dict) and _looks_like_unknown_action(res):
                payload_old = {
                    "api_key": api_key,
                    "type": "bulk",
                    "rows": rows
                }

                res_old = _post_json_to_webapp(payload_old, timeout=60)

                if isinstance(res_old, dict) and res_old.get("ok") is True:
                    return {
                        "ok": True,
                        "mode": "bulk",
                        "updated": res_old.get("updated"),
                        "last_response": res_old
                    }

                return res_old

            return {
                "ok": False,
                "error": "bulk_snapshot_pp failed",
                "detail": res
            }

        wrote_total += int(res.get("wrote") or 0)

    return {
        "ok": True,
        "mode": "bulk_snapshot_pp",
        "snapshot_id": snapshot_id,
        "blocks": blocks,
        "wrote_total": wrote_total
    }


# =======================
# HELPERS
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


def normalize_date_iso(s: str):
    if not s:
        return None

    s = str(s).strip()

    if "/" in s:
        try:
            d = datetime.strptime(s, "%d/%m/%y").date()
            return d.isoformat()
        except Exception:
            return s

    return s


def parse_qr_payload(raw: str) -> dict:
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
            "nro_serie": int(data["NS"]),
            "id_producto": int(normalize_id_value(data["PRD"])),
            "descripcion_qr": str(data["DSC"]).strip(),
            "lote": str(data["LOT"]).strip(),
            "creacion": normalize_date_iso(data["FEC"]),
            "vencimiento": normalize_date_iso(data["VTO"]),
        }

    raise ValueError("QR inválido: formato no reconocido.")


# =======================
# POSTGRES
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
        password=cfg["password"]
    )

    conn.autocommit = True

    enc = cfg.get("client_encoding")
    if enc:
        conn.set_client_encoding(enc)

    return conn


def product_exists(conn, id_producto: int) -> bool:
    cfg = get_pg_config()
    schema = cfg["schema"]
    prod = cfg["table_products"]

    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT 1
            FROM {schema}.{prod}
            WHERE id = %s
            LIMIT 1;
            """,
            (int(id_producto),)
        )

        return cur.fetchone() is not None


def get_product_desc(conn, id_producto: int) -> str:
    cfg = get_pg_config()
    schema = cfg["schema"]
    prod = cfg["table_products"]

    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT descripcion
            FROM {schema}.{prod}
            WHERE id = %s;
            """,
            (int(id_producto),)
        )

        row = cur.fetchone()
        return str(row[0]).strip() if row else ""


def qr_already_scanned(conn, id_producto: int, nro_serie: int, lote: str, descripcion_qr: str) -> bool:
    cfg = get_pg_config()
    schema = cfg["schema"]
    tstock = cfg["table_stock"]
    tprod = cfg["table_products"]

    desc = (descripcion_qr or "").strip()

    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT 1
            FROM {schema}.{tstock} s
            JOIN {schema}.{tprod} p ON p.id = s.id_producto
            WHERE
            (
                s.id_producto = %s
                AND s.nro_serie = %s
                AND s.lote = %s
            )
            OR
            (
                LOWER(TRIM(p.descripcion)) = LOWER(TRIM(%s))
                AND s.nro_serie = %s
                AND s.lote = %s
            )
            LIMIT 1;
            """,
            (
                int(id_producto),
                int(nro_serie),
                str(lote),
                desc,
                int(nro_serie),
                str(lote)
            )
        )

        return cur.fetchone() is not None


def insert_one(conn, id_producto: int, nro_serie: int, lote: str, creacion_iso, venc_iso, tipo_unidad: str, packs: int):
    cfg = get_pg_config()
    schema = cfg["schema"]
    tstock = cfg["table_stock"]

    with conn.cursor() as cur:
        cur.execute(
            f"""
            INSERT INTO {schema}.{tstock}
            (id_producto, nro_serie, lote, creacion, vencimiento, tipo_unidad, packs)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT DO NOTHING;
            """,
            (
                int(id_producto),
                int(nro_serie),
                str(lote),
                creacion_iso,
                venc_iso,
                str(tipo_unidad),
                int(packs)
            )
        )

        return cur.rowcount == 1


def get_last_registered_serial(conn, id_producto: int, lote: str):
    cfg = get_pg_config()
    schema = cfg["schema"]
    tstock = cfg["table_stock"]

    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT MAX(nro_serie)
            FROM {schema}.{tstock}
            WHERE id_producto = %s
              AND lote = %s;
            """,
            (int(id_producto), str(lote))
        )

        row = cur.fetchone()

        if row and row[0] is not None:
            return int(row[0])

        return None


def insert_missing_between(conn, id_producto: int, lote: str, a: int, b: int, creacion_iso, venc_iso):
    lo = min(int(a), int(b))
    hi = max(int(a), int(b))

    gap = hi - lo - 1

    if gap <= 0:
        return 0, 0

    if gap > MAX_AUTOFILL:
        raise ValueError(f"Gap muy grande ({gap}). Para evitar errores no autocompleto más de {MAX_AUTOFILL}.")

    ins = 0
    skip = 0

    for ns in range(lo + 1, hi):
        ok = insert_one(
            conn,
            id_producto=id_producto,
            nro_serie=ns,
            lote=lote,
            creacion_iso=creacion_iso,
            venc_iso=venc_iso,
            tipo_unidad="PALLET",
            packs=0
        )

        if ok:
            ins += 1
        else:
            skip += 1

    return ins, skip


# =======================
# STOCK REAL PARA GOOGLE SHEET
# =======================
def fetch_all_sheet_rows(conn):
    """
    Arma las filas para enviar al Google Sheet directamente desde produccion.stock.

    No usa produccion.sheet.
    No necesita que exista la tabla produccion.sheet.

    stock_pallets = cantidad de registros existentes en produccion.stock por producto.
    stock_packs = suma de packs registrados en produccion.stock por producto.
    """
    cfg = get_pg_config()
    schema = cfg["schema"]
    tstock = cfg["table_stock"]
    tprod = cfg["table_products"]

    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT
                p.id AS codigo,
                p.descripcion,
                COALESCE(st.stock_pallets, 0) AS stock_pallets,
                COALESCE(st.stock_packs, 0) AS stock_packs
            FROM {schema}.{tprod} p
            LEFT JOIN (
                SELECT
                    id_producto,
                    COUNT(*) AS stock_pallets,
                    SUM(COALESCE(packs, 0)) AS stock_packs
                FROM {schema}.{tstock}
                GROUP BY id_producto
            ) st ON st.id_producto = p.id
            ORDER BY p.id;
            """
        )

        rows = cur.fetchall()
        out = []

        for codigo, desc, pallets, packs in rows:
            pallets_val = int(pallets or 0)
            packs_val = int(packs or 0)

            out.append({
                "codigo": int(codigo),
                "id_producto": int(codigo),
                "descripcion": str(desc).strip(),
                "stock_pallets": pallets_val,
                "stock_packs": packs_val,
                "pallet": pallets_val,
                "bulto": packs_val,
            })

        return out


# =======================
# AUTO BULK SYNC
# =======================
_bulk_sync_timer = None
_bulk_sync_lock = threading.Lock()


def auto_sync_bulk_debounced(on_warn=None, delay_seconds: float = 4.0):
    """
    Sincroniza automáticamente el Google Sheet después de cambios en stock.

    Usa debounce: si se escanean varios QR seguidos, espera unos segundos
    y manda un solo snapshot completo al Sheet.
    """
    global _bulk_sync_timer

    def _run():
        conn_sync = None

        try:
            conn_sync = pg_connect()
            rows = fetch_all_sheet_rows(conn_sync)
            res = send_bulk_to_sheet(rows)

            if not (isinstance(res, dict) and res.get("ok") is True):
                if on_warn:
                    on_warn(f"⚠️ Auto-sync Sheet: respuesta inválida: {res}")

        except Exception as e:
            if on_warn:
                on_warn(f"⚠️ Auto-sync Sheet error: {e}")

        finally:
            try:
                if conn_sync is not None:
                    conn_sync.close()
            except Exception:
                pass

    with _bulk_sync_lock:
        if _bulk_sync_timer is not None:
            _bulk_sync_timer.cancel()

        _bulk_sync_timer = threading.Timer(delay_seconds, _run)
        _bulk_sync_timer.daemon = True
        _bulk_sync_timer.start()


# =======================
# UI
# =======================
def main():
    global root, status_var

    conn = pg_connect()

    root = tb.Window(themename="minty")
    root.title("Escáner QRs - Stock Talca")
    root.geometry("980x650")

    tb.Label(
        root,
        text="Escaneo de ENTRADA",
        font=("Segoe UI", 20, "bold")
    ).pack(pady=14)

    scan_var = tb.StringVar()

    entry_scan = tb.Entry(
        root,
        textvariable=scan_var,
        width=98,
        font=("Segoe UI", 14)
    )
    entry_scan.pack(pady=12)
    entry_scan.focus_set()

    is_complete_var = tb.BooleanVar(value=False)

    toggle = tb.Checkbutton(
        root,
        text="Activar si el pallet es completo",
        variable=is_complete_var,
        bootstyle="success-round-toggle"
    )
    toggle.pack(pady=6)

    packs_frame = tb.Frame(root)
    packs_frame.pack(pady=6)

    tb.Label(
        packs_frame,
        text="Cantidad de packs (indicar solo si el pallet es parcial):",
        font=("Segoe UI", 11)
    ).pack(side="left", padx=(0, 8))

    packs_var = tb.StringVar(value="")

    entry_packs = tb.Entry(
        packs_frame,
        textvariable=packs_var,
        width=10,
        font=("Segoe UI", 12)
    )
    entry_packs.pack(side="left")

    status_var = tb.StringVar(value="🟢 Listo: escaneá un QR y Enter.")

    tb.Label(
        root,
        textvariable=status_var,
        font=("Segoe UI", 11),
        justify="left",
        wraplength=940
    ).pack(pady=14)

    pending = {"data": None}

    def _on_sheet_warn(msg):
        def _update():
            current = status_var.get()
            status_var.set(f"{current}\n{msg}")

        root.after(0, _update)

    def set_packs_state():
        if is_complete_var.get():
            entry_packs.configure(state="disabled")
            packs_var.set("")
        else:
            entry_packs.configure(state="normal")

    def reset_after_commit():
        scan_var.set("")
        packs_var.set("")
        pending["data"] = None
        entry_scan.focus_set()

    def format_qr_detail(pid, serie, dsc_qr, lote, cre, vto):
        return (
            f"Número de Serie: {serie}\n"
            f"ID Producto: {pid}\n"
            f"Descripción: {dsc_qr}\n"
            f"Lote: {lote}\n"
            f"Fecha de creación: {cre}\n"
            f"Fecha de vencimiento: {vto}"
        )

    def commit_scan(data: dict, unit_type: str, packs: int):
        pid = int(data["id_producto"])
        lote = str(data["lote"]).strip()
        serie = int(data["nro_serie"])
        cre = data["creacion"]
        vto = data["vencimiento"]
        dsc_qr = str(data.get("descripcion_qr") or "").strip()

        if not product_exists(conn, pid):
            raise ValueError(f"El producto {pid} no existe en produccion.productos.")

        if qr_already_scanned(conn, pid, serie, lote, dsc_qr):
            root.bell()
            status_var.set(
                "ERROR\n"
                "Motivo: Este QR ya fue registrado.\n\n"
                + format_qr_detail(pid, serie, dsc_qr, lote, cre, vto)
            )
            reset_after_commit()
            return

        last_registered = get_last_registered_serial(conn, pid, lote)

        inserted = insert_one(
            conn,
            pid,
            serie,
            lote,
            cre,
            vto,
            "PALLET",
            0
        )

        if not inserted:
            root.bell()
            status_var.set(
                "ERROR\n"
                "Motivo: Este QR ya fue registrado.\n\n"
                + format_qr_detail(pid, serie, dsc_qr, lote, cre, vto)
            )
            reset_after_commit()
            return

        if unit_type == "PACKS" and packs > 0:
            cfg = get_pg_config()

            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    UPDATE {cfg['schema']}.{cfg['table_stock']}
                    SET packs = %s
                    WHERE id_producto = %s
                      AND nro_serie = %s
                      AND lote = %s;
                    """,
                    (
                        int(packs),
                        int(pid),
                        int(serie),
                        str(lote)
                    )
                )

        anchor = 0 if last_registered is None else int(last_registered)

        inserted_between = 0
        skipped_between = 0

        if serie > anchor:
            inserted_between, skipped_between = insert_missing_between(
                conn,
                pid,
                lote,
                anchor,
                serie,
                cre,
                vto
            )

        auto_sync_bulk_debounced(on_warn=_on_sheet_warn)

        root.bell()

        unit_label = f"PARCIAL ({packs} packs)" if unit_type == "PACKS" else "PALLET completo"

        if last_registered is None:
            autofill_msg = f"Autocompletado inicial: 1 a {serie}."
        elif serie > last_registered:
            autofill_msg = f"Autocompletado desde {last_registered} hasta {serie}."
        else:
            autofill_msg = f"Sin autocompletar (el último registrado era {last_registered})."

        status_var.set(
            f"ENVIADO A BD [{unit_label}] - Sheet sincronizando automáticamente...\n"
            f"{autofill_msg}\n"
            f"Insertados intermedios: {inserted_between} | Ya existentes/saltados: {skipped_between}\n\n"
            + format_qr_detail(pid, serie, dsc_qr, lote, cre, vto)
        )

        if unit_type == "PALLET":
            is_complete_var.set(False)

        reset_after_commit()

    def on_scan_enter(event=None):
        raw = scan_var.get().strip()

        if not raw:
            return

        scan_var.set("")

        try:
            data = parse_qr_payload(raw)

            if is_complete_var.get():
                commit_scan(data, "PALLET", 0)
                return

            packs_raw = packs_var.get().strip()

            if packs_raw:
                try:
                    packs_val = int(packs_raw)

                    if packs_val > 0:
                        commit_scan(data, "PACKS", packs_val)
                        return

                except ValueError:
                    pass

            pending["data"] = data

            pid = int(data["id_producto"])
            lote = str(data["lote"]).strip()
            serie = int(data["nro_serie"])
            cre = data["creacion"]
            vto = data["vencimiento"]
            dsc_qr = str(data.get("descripcion_qr") or "").strip()

            status_var.set(
                "QR leído (PARCIAL). Cargá packs y Enter.\n\n"
                + format_qr_detail(pid, serie, dsc_qr, lote, cre, vto)
            )

            entry_packs.focus_set()

        except Exception as e:
            root.bell()
            status_var.set(f"❌ ERROR: {e}")
            pending["data"] = None
            entry_scan.focus_set()

    def on_packs_enter(event=None):
        data = pending.get("data")

        if not data:
            entry_scan.focus_set()
            return

        try:
            packs_val = int(packs_var.get())

            if packs_val <= 0:
                raise ValueError("La cantidad de packs debe ser mayor a 0.")

            commit_scan(data, "PACKS", packs_val)

        except Exception as e:
            root.bell()
            status_var.set(f"❌ ERROR: {e}")
            pending["data"] = None
            entry_scan.focus_set()

    is_complete_var.trace_add("write", lambda *_: set_packs_state())

    entry_scan.bind("<Return>", on_scan_enter)
    entry_packs.bind("<Return>", on_packs_enter)

    set_packs_state()

    # Sincronización inicial automática al abrir el programa.
    # Esto deja el Google Sheet igual que el stock real de la BD aunque no se toque ningún botón.
    auto_sync_bulk_debounced(on_warn=_on_sheet_warn, delay_seconds=1.0)

    def on_close():
        global _bulk_sync_timer

        try:
            if _bulk_sync_timer is not None:
                _bulk_sync_timer.cancel()
        except Exception:
            pass

        try:
            conn.close()
        except Exception:
            pass

        root.destroy()

    root.protocol("WM_DELETE_WINDOW", on_close)
    root.mainloop()


if __name__ == "__main__":
    main()