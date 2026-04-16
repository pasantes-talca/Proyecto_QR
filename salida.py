import os
import sys
import json
import urllib.request
import urllib.error
import threading

import ttkbootstrap as tb
from ttkbootstrap.constants import *
from tkinter import messagebox

try:
    import psycopg2
except Exception:
    psycopg2 = None


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
    "host":             "10.242.4.13",
    "port":             5432,
    "dbname":           "stock_copia",
    "user":             "postgres",
    "password":         "Talca2025",
    "client_encoding":  "WIN1252",
    "schema":           "produccion",
    "table_products":   "productos",
    "table_stock":      "stock",
    "table_bajas":      "bajas",
    "table_sheet":      "sheet",
}

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
    url     = sheet.get("webapp_url") or SHEETS_WEBAPP_URL
    api_key = sheet.get("api_key")    or SHEETS_API_KEY
    return url, api_key


def pg_connect():
    if psycopg2 is None:
        raise RuntimeError("Falta psycopg2. Instalá con: pip install psycopg2-binary")

    cfg  = get_pg_config()
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
    """Asegura columnas necesarias en bajas: motivo, observaciones, tipo_unidad."""
    cfg    = get_pg_config()
    schema = cfg["schema"]
    tbajas = cfg["table_bajas"]

    with conn.cursor() as cur:
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")

        for col, definition in [
            ("motivo",        "TEXT NOT NULL DEFAULT 'Venta'"),
            ("observaciones", "TEXT"),
            ("tipo_unidad",   "TEXT"),
        ]:
            cur.execute(f"""
                DO $$
                BEGIN
                    IF NOT EXISTS (
                        SELECT 1 FROM information_schema.columns
                        WHERE table_schema = '{schema}'
                          AND table_name   = '{tbajas}'
                          AND column_name  = '{col}'
                    ) THEN
                        ALTER TABLE {schema}.{tbajas}
                        ADD COLUMN {col} {definition};
                    END IF;
                END $$;
            """)


# =======================
#   QR PARSER
# =======================
def parse_qr_payload(raw: str) -> dict:
    raw = raw.strip()
    if "|" in raw and "=" in raw:
        parts = raw.split("|")
        data  = {}
        for p in parts:
            if "=" in p:
                k, v = p.split("=", 1)
                data[k.strip().upper()] = v.strip()

        if not data.get("NS") or not data.get("PRD") or not data.get("LOT"):
            raise ValueError("QR inválido: faltan campos (Número de serie / ID producto / Lote).")

        return {
            "nro_serie":   int(data["NS"]),
            "id_producto": int(data["PRD"]),
            "lote":        str(data["LOT"]).strip(),
        }

    raise ValueError("QR inválido: formato no reconocido.")


# =======================
#   GOOGLE SHEET SYNC
# =======================
def _post_json_to_webapp(payload: dict, timeout: int = 30) -> dict:
    url, _ = get_sheet_settings()
    data    = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    req     = urllib.request.Request(
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
                return {"ok": False, "raw": txt}
    except urllib.error.HTTPError as e:
        try:
            body = e.read().decode("utf-8", errors="ignore")
        except Exception:
            body = ""
        return {"ok": False, "http_status": getattr(e, "code", None), "error": str(e), "raw": body}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def send_update_row_to_sheet(descripcion: str, pallets: int, packs: int) -> dict:
    _, api_key = get_sheet_settings()
    payload = {
        "api_key":      api_key,
        "action":       "scan_pp",
        "type":         "scan_pp",
        "descripcion":  str(descripcion),
        "stock_pallets": int(pallets),
        "stock_packs":   int(packs),
    }
    return _post_json_to_webapp(payload, timeout=20)


# =======================
#   DB HELPERS
# =======================
def get_product_desc(conn, id_producto: int) -> str:
    cfg   = get_pg_config()
    schema = cfg["schema"]
    tprod  = cfg["table_products"]
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT descripcion FROM {schema}.{tprod} WHERE id=%s;",
            (int(id_producto),)
        )
        row = cur.fetchone()
        return str(row[0]).strip() if row and row[0] else "Sin descripción"


def get_products_with_stock(conn):
    cfg    = get_pg_config()
    schema = cfg["schema"]
    tstock = cfg["table_stock"]
    tprod  = cfg["table_products"]
    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT DISTINCT p.id, p.descripcion
            FROM {schema}.{tprod} p
            JOIN {schema}.{tstock} s ON s.id_producto = p.id
            ORDER BY p.id ASC;
        """)
        return cur.fetchall()


def get_lotes_for_product(conn, id_producto: int):
    cfg    = get_pg_config()
    schema = cfg["schema"]
    tstock = cfg["table_stock"]
    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT DISTINCT lote
            FROM {schema}.{tstock}
            WHERE id_producto=%s
            ORDER BY lote ASC;
        """, (int(id_producto),))
        return [r[0] for r in cur.fetchall()]


def qr_exists_in_stock(conn, id_producto: int, lote: str, nro_serie: int):
    cfg    = get_pg_config()
    schema = cfg["schema"]
    tstock = cfg["table_stock"]
    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT tipo_unidad, COALESCE(packs, 0)
            FROM {schema}.{tstock}
            WHERE id_producto=%s AND lote=%s AND nro_serie=%s
            LIMIT 1;
        """, (int(id_producto), str(lote), int(nro_serie)))
        row = cur.fetchone()
        if not row:
            return None
        return (str(row[0]).upper().strip(), int(row[1] or 0))


# =======================
#   STOCK NET (CORREGIDO)
#   Un registro cuenta como PACKS si packs > 0,
#   independientemente del valor de tipo_unidad.
#   Un registro cuenta como PALLET puro solo si packs = 0.
# =======================
def compute_net_available_lote(conn, id_producto: int, lote: str):
    cfg    = get_pg_config()
    schema = cfg["schema"]
    tstock = cfg["table_stock"]

    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT
                COUNT(CASE WHEN COALESCE(packs, 0) = 0 THEN 1 END)            AS pallets,
                COALESCE(SUM(CASE WHEN COALESCE(packs, 0) > 0 THEN packs END), 0) AS packs
            FROM {schema}.{tstock}
            WHERE id_producto = %s AND lote = %s;
        """, (int(id_producto), str(lote)))
        row = cur.fetchone()
        return int(row[0] or 0), int(row[1] or 0)


def get_product_net_stock(conn, id_producto: int):
    cfg    = get_pg_config()
    schema = cfg["schema"]
    tprod  = cfg["table_products"]
    tstock = cfg["table_stock"]

    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT
                p.descripcion,
                COUNT(CASE WHEN COALESCE(s.packs, 0) = 0 THEN 1 END)                AS pallets,
                COALESCE(SUM(CASE WHEN COALESCE(s.packs, 0) > 0 THEN s.packs END), 0) AS packs
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
def delete_from_stock_by_qr(conn, id_producto: int, lote: str, nro_serie: int):
    cfg    = get_pg_config()
    schema = cfg["schema"]
    tstock = cfg["table_stock"]

    with conn.cursor() as cur:
        cur.execute(f"""
            DELETE FROM {schema}.{tstock}
            WHERE id_producto=%s AND lote=%s AND nro_serie=%s;
        """, (int(id_producto), str(lote), int(nro_serie)))


def delete_from_stock_manual(conn, id_producto: int, lote: str, tipo_unidad: str, cantidad: int):
    cfg    = get_pg_config()
    schema = cfg["schema"]
    tstock = cfg["table_stock"]

    with conn.cursor() as cur:
        if tipo_unidad == "PALLET":
            # Elimina registros donde packs = 0 (pallets puros)
            cur.execute(f"""
                DELETE FROM {schema}.{tstock}
                WHERE ctid IN (
                    SELECT ctid
                    FROM   {schema}.{tstock}
                    WHERE  id_producto=%s
                      AND  lote=%s
                      AND  COALESCE(packs, 0) = 0
                    ORDER BY nro_serie ASC
                    LIMIT %s
                );
            """, (int(id_producto), str(lote), int(cantidad)))

        else:  # PACKS
            # Selecciona registros donde packs > 0
            cur.execute(f"""
                SELECT nro_serie, COALESCE(packs, 0)
                FROM   {schema}.{tstock}
                WHERE  id_producto=%s AND lote=%s AND COALESCE(packs, 0) > 0
                ORDER  BY nro_serie ASC;
            """, (int(id_producto), str(lote)))
            rows      = cur.fetchall()
            remaining = int(cantidad)

            for ns, packs_val in rows:
                if remaining <= 0:
                    break
                packs_val = int(packs_val or 0)
                if packs_val <= remaining:
                    cur.execute(f"""
                        DELETE FROM {schema}.{tstock}
                        WHERE id_producto=%s AND lote=%s AND nro_serie=%s;
                    """, (int(id_producto), str(lote), ns))
                    remaining -= packs_val
                else:
                    cur.execute(f"""
                        UPDATE {schema}.{tstock}
                        SET    packs = packs - %s
                        WHERE  id_producto=%s AND lote=%s AND nro_serie=%s;
                    """, (remaining, int(id_producto), str(lote), ns))
                    remaining = 0


# =======================
#   SHEET / UPSERT
# =======================
def upsert_sheet(conn, id_producto: int, stock_pallets: int, stock_packs: int):
    cfg    = get_pg_config()
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
                   observaciones: str = None, tipo_unidad: str = None):
    cfg    = get_pg_config()
    schema = cfg["schema"]
    tbajas = cfg["table_bajas"]

    with conn.cursor() as cur:
        cur.execute(f"""
            INSERT INTO {schema}.{tbajas} (
                id_producto, stock_lote, fecha_hora, cantidad,
                motivo, observaciones, tipo_unidad
            ) VALUES (
                %s, %s, NOW(), %s, %s, %s, %s
            )
            RETURNING id;
        """, (
            int(id_producto),
            str(lote),
            int(cantidad),
            str(motivo),
            (observaciones.strip() if observaciones else None),
            (str(tipo_unidad).upper().strip() if tipo_unidad else None),
        ))
        return cur.fetchone()[0]


# =======================
#   REFRESH SHEET (DB rápido + Google en background)
# =======================
def refresh_sheet_everywhere(conn, id_producto: int):
    desc, net_pallets, net_packs = get_product_net_stock(conn, id_producto)
    upsert_sheet(conn, id_producto, net_pallets, net_packs)

    def _sync_google_background():
        warn = ""
        try:
            res = send_update_row_to_sheet(desc, net_pallets, net_packs)
            if not (isinstance(res, dict) and res.get("ok") is True):
                warn = f"⚠️ Google Sheet no confirmó OK: {res}"
        except Exception as e:
            warn = f"⚠️ Error al sync con Google Sheet: {e}"

        if warn:
            def _update_ui():
                current = status_var.get()
                status_var.set(f"{current}\n{warn}")
            root.after(0, _update_ui)

    threading.Thread(target=_sync_google_background, daemon=True).start()

    return net_pallets, net_packs, desc, ""


# =======================
#   BAJAS (lógica corregida)
# =======================
def baja_por_qr(conn, raw_payload: str, motivo: str, observaciones: str = None):
    qr  = parse_qr_payload(raw_payload)
    pid = int(qr["id_producto"])
    lote = str(qr["lote"])
    ns   = int(qr["nro_serie"])

    stock_info = qr_exists_in_stock(conn, pid, lote, ns)
    if not stock_info:
        raise ValueError("Ese QR NO existe en STOCK.")

    tipo_unidad_db, packs = stock_info

    # CORREGIDO: si el registro tiene packs > 0, es PACKS aunque tipo_unidad diga PALLET
    if packs > 0:
        cantidad    = packs
        tipo_unidad = "PACKS"
    else:
        tipo_unidad = "PALLET"
        cantidad    = 1

    net_pallets_lote, net_packs_lote = compute_net_available_lote(conn, pid, lote)
    if tipo_unidad == "PALLET" and net_pallets_lote < 1:
        raise ValueError("No hay PALLETS disponibles para ese lote.")
    if tipo_unidad == "PACKS" and net_packs_lote < cantidad:
        raise ValueError("No hay PACKS disponibles para ese lote.")

    baja_id = registrar_baja(conn, pid, lote, cantidad, motivo, observaciones, tipo_unidad=tipo_unidad)
    delete_from_stock_by_qr(conn, pid, lote, ns)
    net_pallets, net_packs, desc, _ = refresh_sheet_everywhere(conn, pid)

    return baja_id, pid, desc, lote, ns, tipo_unidad, cantidad, net_pallets, net_packs


def baja_manual(conn, id_producto: int, lote: str, tipo: str, cantidad: int,
                motivo: str, observaciones: str = None):
    pid      = int(id_producto)
    lote     = str(lote).strip()
    tipo     = (tipo or "").strip().lower()
    cantidad = int(cantidad)

    if tipo not in ("pallet", "packs"):
        raise ValueError("Tipo inválido (pallet / packs).")
    if cantidad <= 0:
        raise ValueError("Cantidad debe ser > 0.")

    tipo_unidad = "PALLET" if tipo == "pallet" else "PACKS"

    net_pallets_lote, net_packs_lote = compute_net_available_lote(conn, pid, lote)
    if tipo_unidad == "PALLET" and net_pallets_lote < cantidad:
        raise ValueError(f"No hay pallets suficientes en ese lote. Disponibles: {net_pallets_lote}")
    if tipo_unidad == "PACKS" and net_packs_lote < cantidad:
        raise ValueError(f"No hay packs suficientes en ese lote. Disponibles: {net_packs_lote}")

    baja_id = registrar_baja(conn, pid, lote, cantidad, motivo, observaciones, tipo_unidad=tipo_unidad)
    delete_from_stock_manual(conn, pid, lote, tipo_unidad, cantidad)
    net_pallets, net_packs, desc, _ = refresh_sheet_everywhere(conn, pid)

    return baja_id, pid, desc, lote, tipo_unidad, cantidad, net_pallets, net_packs


# =======================
#   UI
# =======================
MOTIVOS = ("Venta", "Calidad", "Desarme", "Observacion")


def main():
    global root, status_var

    try:
        conn = pg_connect()
        init_tables(conn)
    except Exception as e:
        messagebox.showerror("Error PostgreSQL", f"No se pudo conectar:\n{e}")
        return

    root = tb.Window(themename="minty")
    root.title("Baja por QR / Manual – Talca (OPTIMIZADO)")
    root.geometry("1100x760")

    container = tb.Frame(root)
    container.pack(expand=True)

    tb.Label(container, text="Baja por QR o Manual",
             font=("Segoe UI", 22, "bold")).pack(pady=16)

    # ── Motivo ────────────────────────────────────────────────────────────────
    motivo_var   = tb.StringVar(value="Venta")
    frame_motivo = tb.Frame(container)
    frame_motivo.pack(pady=8)
    tb.Label(frame_motivo, text="Motivo de baja:").pack(side="left", padx=10)
    for m in MOTIVOS:
        tb.Radiobutton(frame_motivo, text=m, variable=motivo_var, value=m).pack(side="left", padx=10)

    # ══════════════════════════════════════════════════════════════════════════
    #   SECCIÓN QR  (envío automático, sin botón)
    # ══════════════════════════════════════════════════════════════════════════
    tb.Label(container,
             text="Carga por QR – escaneá con pistola lectora (se envía automáticamente)",
             font=("Segoe UI", 14)).pack(pady=(16, 4))

    # ── Observaciones para QR ─────────────────────────────────────────────────
    frame_obs_qr = tb.Frame(container)
    frame_obs_qr.pack(pady=(0, 4))
    tb.Label(frame_obs_qr, text="Observación QR (opcional):",
             font=("Segoe UI", 11)).pack(side="left", padx=8)
    obs_qr_var   = tb.StringVar()
    obs_qr_entry = tb.Entry(frame_obs_qr, textvariable=obs_qr_var,
                            width=55, font=("Segoe UI", 11))
    obs_qr_entry.pack(side="left", padx=4)

    qr_var   = tb.StringVar()
    qr_entry = tb.Entry(container, textvariable=qr_var, width=80, font=("Segoe UI", 14))
    qr_entry.pack(pady=(4, 12))
    qr_entry.focus_set()

    # ══════════════════════════════════════════════════════════════════════════
    #   SECCIÓN MANUAL  (envío con botón ENVIAR)
    # ══════════════════════════════════════════════════════════════════════════
    tb.Label(container, text="Carga Manual",
             font=("Segoe UI", 14)).pack(pady=(8, 10))

    frame_manual = tb.Frame(container)
    frame_manual.pack(pady=8)

    tb.Label(frame_manual, text="Producto:").grid(row=0, column=0, sticky="e", padx=12, pady=8)
    prods    = get_products_with_stock(conn)
    options  = [f"{pid} - {desc}" for pid, desc in prods]
    prod_var = tb.StringVar()
    prod_combo = tb.Combobox(frame_manual, textvariable=prod_var, values=options,
                             width=60, state="readonly")
    prod_combo.grid(row=0, column=1, columnspan=3, sticky="w", padx=12, pady=8)

    tb.Label(frame_manual, text="Lote:").grid(row=1, column=0, sticky="e", padx=12, pady=8)
    lote_var   = tb.StringVar()
    lote_combo = tb.Combobox(frame_manual, textvariable=lote_var, width=30, state="readonly")
    lote_combo.grid(row=1, column=1, sticky="w", padx=12, pady=8)

    # ── Disponible del lote seleccionado ─────────────────────────────────────
    lote_stock_var = tb.StringVar(value="")
    tb.Label(frame_manual, textvariable=lote_stock_var,
             font=("Segoe UI", 10), foreground="#555").grid(
        row=1, column=2, columnspan=2, sticky="w", padx=12)

    tb.Label(frame_manual, text="Tipo:").grid(row=2, column=0, sticky="e", padx=12, pady=8)
    type_var = tb.StringVar(value="pallet")
    tb.Radiobutton(frame_manual, text="Pallets", variable=type_var,
                   value="pallet").grid(row=2, column=1, sticky="w", padx=12)
    tb.Radiobutton(frame_manual, text="Packs", variable=type_var,
                   value="packs").grid(row=2, column=2, sticky="w")

    tb.Label(frame_manual, text="Cantidad:").grid(row=3, column=0, sticky="e", padx=12, pady=8)
    cant_var = tb.StringVar(value="")
    tb.Entry(frame_manual, textvariable=cant_var, width=12).grid(
        row=3, column=1, sticky="w", padx=12)

    tb.Label(container,
             text="Observaciones Manual (opcional):",
             font=("Segoe UI", 12)).pack(pady=4)
    obs_manual_var   = tb.StringVar()
    obs_manual_entry = tb.Entry(container, textvariable=obs_manual_var,
                                width=90, font=("Segoe UI", 12))
    obs_manual_entry.pack(pady=(0, 12))

    # ── Botón ENVIAR  (solo manual) ───────────────────────────────────────────
    btn_send = tb.Button(container, text="ENVIAR BAJA MANUAL",
                         bootstyle=WARNING, width=28)
    btn_send.pack(pady=(6, 12))

    # ── Estado ────────────────────────────────────────────────────────────────
    status_var   = tb.StringVar(
        value="🟢 Listo – escaneá un QR o completá la carga manual.")
    status_label = tb.Label(container, textvariable=status_var,
                            font=("Segoe UI", 12), wraplength=900, justify="center")
    status_label.pack(pady=(0, 10), padx=40)

    def update_wrap(event=None):
        try:
            w = root.winfo_width()
            status_label.configure(wraplength=max(500, w - 200))
        except Exception:
            pass

    root.bind("<Configure>", update_wrap)

    # ── Helpers ───────────────────────────────────────────────────────────────
    def refresh_lote_stock_label():
        """Muestra pallets y packs disponibles para el lote seleccionado."""
        val  = prod_var.get()
        lote = lote_var.get().strip()
        if not val or not lote:
            lote_stock_var.set("")
            return
        try:
            pid = int(val.split(" - ")[0])
            p, pk = compute_net_available_lote(conn, pid, lote)
            lote_stock_var.set(f"Disponible → Pallets: {p}  |  Packs: {pk}")
        except Exception:
            lote_stock_var.set("")

    def on_product_select(event=None):
        val = prod_var.get()
        if not val:
            lote_combo["values"] = []
            lote_var.set("")
            lote_stock_var.set("")
            return
        try:
            pid   = int(val.split(" - ")[0])
            lotes = get_lotes_for_product(conn, pid)
            lote_combo["values"] = lotes
            lote_var.set(lotes[0] if lotes else "")
            refresh_lote_stock_label()
        except Exception:
            lote_combo["values"] = []
            lote_var.set("")
            lote_stock_var.set("")

    def on_lote_select(event=None):
        refresh_lote_stock_label()

    prod_combo.bind("<<ComboboxSelected>>", on_product_select)
    lote_combo.bind("<<ComboboxSelected>>", on_lote_select)

    def refresh_product_combo():
        new_prods   = get_products_with_stock(conn)
        new_options = [f"{pid} - {desc}" for pid, desc in new_prods]
        prod_combo["values"] = new_options

    # ── Envío por QR  (AUTOMÁTICO al escanear) ────────────────────────────────
    def on_qr_scan(event=None):
        raw = qr_var.get().strip()
        if not raw:
            return

        try:
            parse_qr_payload(raw)
        except Exception as e:
            status_var.set(f"❌ ERROR: QR inválido – {e}")
            qr_var.set("")
            qr_entry.focus_set()
            return

        try:
            motivo = motivo_var.get()
            obs    = obs_qr_var.get().strip() or None

            (baja_id, pid, desc, lote, ns,
             tipo_unidad, cantidad,
             net_p, net_pk) = baja_por_qr(conn, raw, motivo, observaciones=obs)

            obs_txt = obs if obs else "Ninguna"
            status_var.set(
                f"✅ Baja por QR registrada automáticamente\n"
                f"ID baja: {baja_id} | Producto: {pid} – {desc}\n"
                f"Lote: {lote} | Serie: {ns} | {tipo_unidad}: {cantidad}\n"
                f"Motivo: {motivo} | Observación: {obs_txt}\n"
                f"Stock total restante → Pallets: {net_p} | Packs: {net_pk}"
            )
            refresh_product_combo()
            refresh_lote_stock_label()

        except Exception as e:
            status_var.set(f"❌ ERROR al registrar baja por QR: {e}")

        finally:
            qr_var.set("")
            qr_entry.focus_set()

    qr_entry.bind("<Return>", on_qr_scan)

    # ── Envío manual  (botón ENVIAR) ──────────────────────────────────────────
    def submit_manual():
        try:
            pstr = prod_var.get()
            if not pstr:
                raise ValueError("Selecciona un producto.")
            pid = int(pstr.split(" - ")[0])

            lote = lote_var.get().strip()
            if not lote:
                raise ValueError("Selecciona un lote.")

            tipo    = type_var.get()
            qty_str = cant_var.get().strip()
            if not qty_str.isdigit():
                raise ValueError("Cantidad debe ser un número entero.")
            qty = int(qty_str)
            if qty <= 0:
                raise ValueError("Cantidad debe ser mayor que 0.")

            motivo = motivo_var.get()
            obs    = obs_manual_var.get().strip() or None

            (baja_id, pid, desc, lote, tipo_unidad,
             cantidad, net_p, net_pk) = baja_manual(
                conn, pid, lote, tipo, qty, motivo, obs)

            obs_txt = obs if obs else "Ninguna"
            status_var.set(
                f"✅ Baja manual registrada\n"
                f"ID baja: {baja_id} | Producto: {pid} – {desc}\n"
                f"Lote: {lote} | {tipo_unidad}: {cantidad}\n"
                f"Motivo: {motivo} | Obs: {obs_txt}\n"
                f"Stock total restante → Pallets: {net_p} | Packs: {net_pk}"
            )

            cant_var.set("")
            obs_manual_var.set("")
            refresh_product_combo()
            on_product_select()
            qr_entry.focus_set()

        except Exception as e:
            status_var.set(f"❌ ERROR al registrar baja manual: {e}")

    btn_send.configure(command=submit_manual)
    obs_manual_entry.bind("<Return>", lambda e: submit_manual())

    # Init combos
    if options:
        prod_var.set(options[0])
        on_product_select()

    root.mainloop()


if __name__ == "__main__":
    main()