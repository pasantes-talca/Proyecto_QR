import os
import sys
import json
import urllib.request
import urllib.error

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
    "host": os.getenv("TALCA_PG_HOST", "localhost"),
    "port": int(os.getenv("TALCA_PG_PORT", "5432")),
    "dbname": os.getenv("TALCA_PG_DB", "postgres"),
    "user": os.getenv("TALCA_PG_USER", "postgres"),
    "password": os.getenv("TALCA_PG_PASS", ""),
    "client_encoding": os.getenv("TALCA_PG_ENCODING", ""),
    "schema": "produccion",
    "table_products": "productos",
    "table_stock": "stock",
    "table_bajas": "productos_bajas",
    "table_sheet": "sheet",
}

# =======================
#   GOOGLE SHEET WEBAPP
# =======================
# Si querés moverlo a config.json:
# "sheet": {"webapp_url":"...", "api_key":"..."}
SHEETS_WEBAPP_URL = "https://script.google.com/macros/s/AKfycbwwzMiTB7DEbcOdvi5Vl32xF-McguAlgkzcBQoeAGhzlowc5J1PjF1QLChNcukf5fbn/exec"
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
    Asegura columnas necesarias en productos_bajas:
      motivo, observaciones, tipo_unidad
    (NO crea nro_serie porque vos lo eliminaste.)
    """
    cfg = get_pg_config()
    schema = cfg["schema"]
    tbajas = cfg["table_bajas"]

    with conn.cursor() as cur:
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")

        # motivo
        cur.execute(f"""
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_schema = '{schema}'
                      AND table_name = '{tbajas}'
                      AND column_name = 'motivo'
                ) THEN
                    ALTER TABLE {schema}.{tbajas}
                    ADD COLUMN motivo TEXT NOT NULL DEFAULT 'Venta';
                END IF;
            END $$;
        """)

        # observaciones
        cur.execute(f"""
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_schema = '{schema}'
                      AND table_name = '{tbajas}'
                      AND column_name = 'observaciones'
                ) THEN
                    ALTER TABLE {schema}.{tbajas}
                    ADD COLUMN observaciones TEXT;
                END IF;
            END $$;
        """)

        # tipo_unidad (PALLET / PACKS)
        cur.execute(f"""
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_schema = '{schema}'
                      AND table_name = '{tbajas}'
                      AND column_name = 'tipo_unidad'
                ) THEN
                    ALTER TABLE {schema}.{tbajas}
                    ADD COLUMN tipo_unidad TEXT;
                END IF;
            END $$;
        """)


# =======================
#   QR PARSER
# =======================
def parse_qr_payload(raw: str) -> dict:
    """
    Formato esperado:
      NS=000001|PRD=4910|DSC=...|LOT=240226|FEC=...|VTO=...
    """
    raw = raw.strip()
    if "|" in raw and "=" in raw:
        parts = raw.split("|")
        data = {}
        for p in parts:
            if "=" in p:
                k, v = p.split("=", 1)
                data[k.strip().upper()] = v.strip()

        if not data.get("NS") or not data.get("PRD") or not data.get("LOT"):
            raise ValueError("QR inválido: faltan campos (Número de serie / Identificador de producto / Lote).")

        return {
            "nro_serie": int(data["NS"]),
            "id_producto": int(data["PRD"]),
            "lote": str(data["LOT"]).strip(),
        }

    raise ValueError("QR inválido: formato no reconocido.")


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
        "api_key": api_key,
        "action": "scan_pp",
        "type": "scan_pp",
        "descripcion": str(descripcion),
        "stock_pallets": int(pallets),
        "stock_packs": int(packs),
    }
    return _post_json_to_webapp(payload, timeout=20)


# =======================
#   DB HELPERS
# =======================
def get_product_desc(conn, id_producto: int) -> str:
    cfg = get_pg_config()
    schema = cfg["schema"]
    tprod = cfg["table_products"]
    with conn.cursor() as cur:
        cur.execute(f"SELECT descripcion FROM {schema}.{tprod} WHERE id=%s;", (int(id_producto),))
        row = cur.fetchone()
        return str(row[0]).strip() if row and row[0] else "Sin descripción"


def get_products_with_stock(conn):
    cfg = get_pg_config()
    schema = cfg["schema"]
    tstock = cfg["table_stock"]
    tprod = cfg["table_products"]
    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT DISTINCT p.id, p.descripcion
            FROM {schema}.{tprod} p
            JOIN {schema}.{tstock} s ON s.id_producto = p.id
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
            WHERE id_producto=%s
            ORDER BY lote ASC;
        """, (int(id_producto),))
        return [r[0] for r in cur.fetchall()]


def qr_exists_in_stock(conn, id_producto: int, lote: str, nro_serie: int):
    """
    Verifica que el QR exista en stock (modelo por fila/serie).
    Devuelve (tipo_unidad, packs) o None.
    """
    cfg = get_pg_config()
    schema = cfg["schema"]
    tstock = cfg["table_stock"]

    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT tipo_unidad, COALESCE(packs,0)
            FROM {schema}.{tstock}
            WHERE id_producto=%s AND lote=%s AND nro_serie=%s
            LIMIT 1;
        """, (int(id_producto), str(lote), int(nro_serie)))
        row = cur.fetchone()
        if not row:
            return None
        return (str(row[0]).upper().strip(), int(row[1] or 0))


def compute_net_available_lote(conn, id_producto: int, lote: str):
    """
    Neto disponible por LOTE:
      pallets_net = count(PALLET en stock lote) - sum(bajas PALLET lote)
      packs_net   = sum(packs en stock lote)  - sum(bajas PACKS lote)
    """
    cfg = get_pg_config()
    schema = cfg["schema"]
    tstock = cfg["table_stock"]
    tbajas = cfg["table_bajas"]

    with conn.cursor() as cur:
        # Entradas
        cur.execute(f"""
            SELECT COALESCE(COUNT(*),0)
            FROM {schema}.{tstock}
            WHERE id_producto=%s AND lote=%s AND tipo_unidad='PALLET';
        """, (int(id_producto), str(lote)))
        in_pallets = int(cur.fetchone()[0] or 0)

        cur.execute(f"""
            SELECT COALESCE(SUM(COALESCE(packs,0)),0)
            FROM {schema}.{tstock}
            WHERE id_producto=%s AND lote=%s AND tipo_unidad='PACKS';
        """, (int(id_producto), str(lote)))
        in_packs = int(cur.fetchone()[0] or 0)

        # Bajas
        cur.execute(f"""
            SELECT COALESCE(SUM(cantidad),0)
            FROM {schema}.{tbajas}
            WHERE id_producto=%s AND stock_lote=%s
              AND (tipo_unidad='PALLET' OR tipo_unidad IS NULL);
        """, (int(id_producto), str(lote)))
        out_pallets = int(cur.fetchone()[0] or 0)

        cur.execute(f"""
            SELECT COALESCE(SUM(cantidad),0)
            FROM {schema}.{tbajas}
            WHERE id_producto=%s AND stock_lote=%s
              AND tipo_unidad='PACKS';
        """, (int(id_producto), str(lote)))
        out_packs = int(cur.fetchone()[0] or 0)

    return max(in_pallets - out_pallets, 0), max(in_packs - out_packs, 0)


def compute_net_totals_product(conn, id_producto: int):
    """
    Neto por PRODUCTO (todos los lotes):
      net_pallets = count(PALLET stock) - sum(bajas PALLET)
      net_packs   = sum(packs stock)    - sum(bajas PACKS)
    """
    cfg = get_pg_config()
    schema = cfg["schema"]
    tstock = cfg["table_stock"]
    tbajas = cfg["table_bajas"]

    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT COALESCE(COUNT(*),0)
            FROM {schema}.{tstock}
            WHERE id_producto=%s AND tipo_unidad='PALLET';
        """, (int(id_producto),))
        in_pallets = int(cur.fetchone()[0] or 0)

        cur.execute(f"""
            SELECT COALESCE(SUM(COALESCE(packs,0)),0)
            FROM {schema}.{tstock}
            WHERE id_producto=%s AND tipo_unidad='PACKS';
        """, (int(id_producto),))
        in_packs = int(cur.fetchone()[0] or 0)

        cur.execute(f"""
            SELECT COALESCE(SUM(cantidad),0)
            FROM {schema}.{tbajas}
            WHERE id_producto=%s
              AND (tipo_unidad='PALLET' OR tipo_unidad IS NULL);
        """, (int(id_producto),))
        out_pallets = int(cur.fetchone()[0] or 0)

        cur.execute(f"""
            SELECT COALESCE(SUM(cantidad),0)
            FROM {schema}.{tbajas}
            WHERE id_producto=%s
              AND tipo_unidad='PACKS';
        """, (int(id_producto),))
        out_packs = int(cur.fetchone()[0] or 0)

    return max(in_pallets - out_pallets, 0), max(in_packs - out_packs, 0)


def upsert_sheet(conn, id_producto: int, stock_pallets: int, stock_packs: int):
    cfg = get_pg_config()
    schema = cfg["schema"]
    tsheet = cfg["table_sheet"]

    with conn.cursor() as cur:
        cur.execute(
            f"""
            INSERT INTO {schema}.{tsheet}(id_producto, stock_pallets, stock_packs)
            VALUES (%s, %s, %s)
            ON CONFLICT (id_producto)
            DO UPDATE SET stock_pallets = EXCLUDED.stock_pallets,
                          stock_packs   = EXCLUDED.stock_packs;
            """,
            (int(id_producto), int(stock_pallets), int(stock_packs))
        )


def registrar_baja(conn, id_producto: int, lote: str, cantidad: int, motivo: str,
                   observaciones: str = None, tipo_unidad: str = None):
    """
    Inserta SOLO en productos_bajas (sin nro_serie).
    """
    cfg = get_pg_config()
    schema = cfg["schema"]
    tbajas = cfg["table_bajas"]

    with conn.cursor() as cur:
        cur.execute(f"""
            INSERT INTO {schema}.{tbajas} (
                id_producto,
                stock_lote,
                fecha_hora,
                cantidad,
                motivo,
                observaciones,
                tipo_unidad
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
            (str(tipo_unidad).upper().strip() if tipo_unidad else None)
        ))
        return cur.fetchone()[0]


def refresh_sheet_everywhere(conn, id_producto: int):
    """
    Recalcula NETO por producto, actualiza:
      - Postgres: produccion.sheet
      - Google Sheet: scan_pp (una fila por descripcion)
    """
    desc = get_product_desc(conn, id_producto)
    net_pallets, net_packs = compute_net_totals_product(conn, id_producto)

    upsert_sheet(conn, id_producto, net_pallets, net_packs)

    warn = ""
    try:
        res = send_update_row_to_sheet(desc, net_pallets, net_packs)
        if not (isinstance(res, dict) and res.get("ok") is True):
            warn = f"⚠️ Google Sheet no confirmó OK: {res}"
    except Exception as e:
        warn = f"⚠️ Error al sync con Google Sheet: {e}"

    return net_pallets, net_packs, desc, warn


# =======================
#   BAJAS (QR / MANUAL)
# =======================
def baja_por_qr(conn, raw_payload: str, motivo: str, observaciones: str = None):
    """
    1) Verifica que el QR exista en stock.
    2) Determina tipo_unidad y cantidad:
         - PALLET => cantidad = 1
         - PACKS  => cantidad = packs de esa fila
    3) Valida contra neto del lote.
    4) Inserta en productos_bajas.
    5) Refresca sheet (Postgres + Google Sheet).
    """
    qr = parse_qr_payload(raw_payload)
    pid = int(qr["id_producto"])
    lote = str(qr["lote"])
    ns = int(qr["nro_serie"])  # NO se guarda en bajas, se usa solo para validar existencia en stock.

    stock_info = qr_exists_in_stock(conn, pid, lote, ns)
    if not stock_info:
        raise ValueError("Ese QR NO existe en STOCK (id_producto + lote + nro_serie).")

    tipo_unidad, packs = stock_info
    if tipo_unidad == "PACKS":
        cantidad = packs if packs > 0 else 1
    else:
        tipo_unidad = "PALLET"
        cantidad = 1

    net_pallets_lote, net_packs_lote = compute_net_available_lote(conn, pid, lote)
    if tipo_unidad == "PALLET" and net_pallets_lote < 1:
        raise ValueError("No hay PALLETS netos disponibles para ese lote.")
    if tipo_unidad == "PACKS" and net_packs_lote < cantidad:
        raise ValueError("No hay PACKS netos disponibles para ese lote.")

    baja_id = registrar_baja(conn, pid, lote, cantidad, motivo, observaciones, tipo_unidad=tipo_unidad)
    net_pallets, net_packs, desc, warn = refresh_sheet_everywhere(conn, pid)

    return baja_id, pid, desc, lote, ns, tipo_unidad, cantidad, net_pallets, net_packs, warn


def baja_manual(conn, id_producto: int, lote: str, tipo: str, cantidad: int, motivo: str, observaciones: str = None):
    """
    Manual:
      - tipo pallet/packs
      - valida contra neto del lote
      - inserta en productos_bajas
      - refresca sheet
    """
    pid = int(id_producto)
    lote = str(lote).strip()
    tipo = (tipo or "").strip().lower()
    cantidad = int(cantidad)

    if tipo not in ("pallet", "packs"):
        raise ValueError("Tipo inválido (pallet / packs).")
    if cantidad <= 0:
        raise ValueError("Cantidad debe ser > 0.")

    tipo_unidad = "PALLET" if tipo == "pallet" else "PACKS"

    net_pallets_lote, net_packs_lote = compute_net_available_lote(conn, pid, lote)
    if tipo_unidad == "PALLET" and net_pallets_lote < cantidad:
        raise ValueError(f"No hay pallets netos suficientes en ese lote. Netos: {net_pallets_lote}")
    if tipo_unidad == "PACKS" and net_packs_lote < cantidad:
        raise ValueError(f"No hay packs netos suficientes en ese lote. Netos: {net_packs_lote}")

    baja_id = registrar_baja(conn, pid, lote, cantidad, motivo, observaciones, tipo_unidad=tipo_unidad)
    net_pallets, net_packs, desc, warn = refresh_sheet_everywhere(conn, pid)

    return baja_id, pid, desc, lote, tipo_unidad, cantidad, net_pallets, net_packs, warn


# =======================
#   UI
# =======================
def main():
    try:
        conn = pg_connect()
        init_tables(conn)
    except Exception as e:
        messagebox.showerror("Error PostgreSQL", f"No se pudo conectar:\n{e}")
        return

    root = tb.Window(themename="minty")
    root.title("Baja por QR / Manual – Talca")
    root.geometry("1100x800")

    tb.Label(root, text="Baja por QR o Manual", font=("Segoe UI", 22, "bold")).pack(pady=16)

    # Motivo
    motivo_var = tb.StringVar(value="Venta")
    frame_motivo = tb.Frame(root)
    frame_motivo.pack(pady=8)
    tb.Label(frame_motivo, text="Motivo de baja:").pack(side="left", padx=10)
    tb.Radiobutton(frame_motivo, text="Venta", variable=motivo_var, value="Venta").pack(side="left", padx=10)
    tb.Radiobutton(frame_motivo, text="Calidad", variable=motivo_var, value="Calidad").pack(side="left", padx=10)
    tb.Radiobutton(frame_motivo, text="Desarme", variable=motivo_var, value="Desarme").pack(side="left", padx=10)

    # Observaciones
    tb.Label(root, text="Observaciones (opcional):", font=("Segoe UI", 12)).pack(pady=4)
    obs_var = tb.StringVar()
    tb.Entry(root, textvariable=obs_var, width=90, font=("Segoe UI", 12)).pack(pady=4)

    # Modo QR
    tb.Label(root, text="Modo QR: Escanea con pistola lectora", font=("Segoe UI", 14)).pack(pady=12)
    qr_var = tb.StringVar()
    qr_entry = tb.Entry(root, textvariable=qr_var, width=80, font=("Segoe UI", 14))
    qr_entry.pack(pady=8)
    qr_entry.focus_set()

    # Manual
    tb.Label(root, text="Modo Manual", font=("Segoe UI", 14)).pack(pady=16)
    frame_manual = tb.Frame(root)
    frame_manual.pack(pady=8, padx=40, fill="x")

    tb.Label(frame_manual, text="Producto:").grid(row=0, column=0, sticky="e", padx=12, pady=8)
    prods = get_products_with_stock(conn)
    options = [f"{pid} - {desc}" for pid, desc in prods]
    prod_var = tb.StringVar()
    prod_combo = tb.Combobox(frame_manual, textvariable=prod_var, values=options, width=60)
    prod_combo.grid(row=0, column=1, columnspan=3, sticky="w", padx=12, pady=8)

    tb.Label(frame_manual, text="Lote:").grid(row=1, column=0, sticky="e", padx=12, pady=8)
    lote_var = tb.StringVar()
    lote_combo = tb.Combobox(frame_manual, textvariable=lote_var, width=30)
    lote_combo.grid(row=1, column=1, sticky="w", padx=12, pady=8)

    tb.Label(frame_manual, text="Tipo:").grid(row=2, column=0, sticky="e", padx=12, pady=8)
    type_var = tb.StringVar(value="pallet")
    tb.Radiobutton(frame_manual, text="Pallets", variable=type_var, value="pallet").grid(row=2, column=1, sticky="w", padx=12)
    tb.Radiobutton(frame_manual, text="Packs", variable=type_var, value="packs").grid(row=2, column=2, sticky="w")

    tb.Label(frame_manual, text="Cantidad:").grid(row=3, column=0, sticky="e", padx=12, pady=8)
    cant_var = tb.StringVar(value="")
    tb.Entry(frame_manual, textvariable=cant_var, width=12).grid(row=3, column=1, sticky="w", padx=12)

    status_var = tb.StringVar(value="🟢 Listo – escaneá QR o usa manual.")
    tb.Label(root, textvariable=status_var, font=("Segoe UI", 12), wraplength=900, justify="left").pack(pady=12, padx=40)

    def reset_form():
        motivo_var.set("Venta")
        obs_var.set("")
        cant_var.set("")
        qr_var.set("")
        qr_entry.focus_set()

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

    def on_qr_scan(event=None):
        raw = qr_var.get().strip()
        if not raw:
            return
        qr_var.set("")

        try:
            motivo = motivo_var.get()
            obs = obs_var.get().strip() or None

            baja_id, pid, desc, lote, ns, tipo_unidad, cantidad, net_p, net_pk, warn = baja_por_qr(conn, raw, motivo, obs)

            status_var.set(
                f"✅ Baja por QR registrada\n"
                f"Identificador de baja: {baja_id}\n"
                f"Producto: {pid} - {desc}\n"
                f"Lote: {lote} | Número de serie (solo validación): {ns}\n"
                f"Tipo de unidad: {tipo_unidad} | Cantidad: {cantidad}\n"
                f"Motivo: {motivo}\n"
                f"Observaciones: {obs if obs else 'Ninguna'}\n"
                f"Neto TOTAL producto → Pallets: {net_p} | Packs: {net_pk}\n"
                f"{warn}"
            )
            reset_form()

        except Exception as e:
            status_var.set(f"❌ ERROR al registrar salida por QR: {e}")
            qr_entry.focus_set()

    qr_entry.bind("<Return>", on_qr_scan)

    def on_manual_baja():
        try:
            pstr = prod_var.get()
            if not pstr:
                raise ValueError("Selecciona producto")
            pid = int(pstr.split(" - ")[0])

            lote = lote_var.get().strip()
            if not lote:
                raise ValueError("Selecciona lote")

            tipo = type_var.get()
            qty_str = cant_var.get().strip()
            if not qty_str.isdigit():
                raise ValueError("Cantidad debe ser número")
            qty = int(qty_str)
            if qty <= 0:
                raise ValueError("Cantidad > 0")

            motivo = motivo_var.get()
            obs = obs_var.get().strip() or None

            baja_id, pid, desc, lote, tipo_unidad, cantidad, net_p, net_pk, warn = baja_manual(conn, pid, lote, tipo, qty, motivo, obs)

            status_var.set(
                f"✅ Baja manual registrada\n"
                f"Identificador de baja: {baja_id}\n"
                f"Producto: {pid} - {desc}\n"
                f"Lote: {lote}\n"
                f"Tipo de unidad: {tipo_unidad} | Cantidad: {cantidad}\n"
                f"Motivo: {motivo}\n"
                f"Observaciones: {obs if obs else 'Ninguna'}\n"
                f"Neto TOTAL producto → Pallets: {net_p} | Packs: {net_pk}\n"
                f"{warn}"
            )
            reset_form()

        except Exception as e:
            status_var.set(f"❌ ERROR al registrar salida manual: {e}")

    tb.Button(root, text="EJECUTAR BAJA MANUAL", bootstyle=WARNING, width=25, command=on_manual_baja).pack(pady=16)

    if options:
        prod_var.set(options[0])
        on_product_select()

    root.mainloop()


if __name__ == "__main__":
    main()