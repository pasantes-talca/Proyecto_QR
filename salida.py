import os
import sys
import json
from datetime import datetime
import urllib.request
import urllib.error

import ttkbootstrap as tb
from ttkbootstrap.constants import *
from tkinter import messagebox

try:
    import psycopg2
    from psycopg2.extras import Json
except Exception:
    psycopg2 = None
    Json = None


# =======================
# CONFIGURACIÓN Y PATHS
# =======================
def get_app_dir():
    if getattr(sys, "frozen", False):
        return os.path.dirname(sys.executable)
    return os.path.dirname(os.path.abspath(__file__))


APP_DIR = get_app_dir()
CACHE_FILE = os.path.join(APP_DIR, "config.json")


# =======================
# GOOGLE SHEETS (opcional, si lo usas)
# =======================
SHEETS_WEBAPP_URL = "https://script.google.com/macros/s/AKfycbz3HnhMu8ylXKtiEVvcsIRc_VKJzxUQHotKDOHT74QgTgLIVbJPPiX3eJBly368Ad4/exec"
SHEETS_API_KEY = "TALCA-QR-2026"


# =======================
# POSTGRES CONFIG
# =======================
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
    "table_bajas": "productos_bajas",          # tabla donde guardamos bajas con motivo y obs
    "table_ultimo_serie": "ultimo_serie_por_lote",
}


def load_cache():
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except:
            return {}
    return {}


def get_pg_config():
    cfg = DEFAULT_PG.copy()
    cache = load_cache()
    if isinstance(cache.get("pg"), dict):
        for k, v in cache["pg"].items():
            if v is not None and v != "":
                cfg[k] = v
    try:
        cfg["port"] = int(cfg["port"])
    except:
        cfg["port"] = 5432
    return cfg


# =======================
# CONEXIÓN POSTGRES
# =======================
def pg_connect():
    if psycopg2 is None:
        raise RuntimeError("Instala psycopg2: pip install psycopg2-binary")

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

    with conn.cursor() as cur:
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")

        # Asegurar columnas en productos_bajas (ya las tenés, pero por seguridad)
        cur.execute(f"""
            DO $$
            BEGIN
                -- motivo
                IF NOT EXISTS (
                    SELECT 1 FROM information_schema.columns 
                    WHERE table_schema = '{schema}' 
                      AND table_name = '{cfg['table_bajas']}' 
                      AND column_name = 'motivo'
                ) THEN
                    ALTER TABLE {schema}.{cfg['table_bajas']}
                    ADD COLUMN motivo TEXT NOT NULL DEFAULT 'Venta';
                END IF;

                -- observaciones (opcional)
                IF NOT EXISTS (
                    SELECT 1 FROM information_schema.columns 
                    WHERE table_schema = '{schema}' 
                      AND table_name = '{cfg['table_bajas']}' 
                      AND column_name = 'observaciones'
                ) THEN
                    ALTER TABLE {schema}.{cfg['table_bajas']}
                    ADD COLUMN observaciones TEXT;
                END IF;
            END $$;
        """)


# =======================
# PARSEO QR (ajusta según tu formato real)
# =======================
def parse_qr(raw: str):
    raw = raw.strip()
    if not raw:
        raise ValueError("QR vacío")

    data = {}
    for part in raw.split('|'):
        if '=' in part:
            k, v = part.split('=', 1)
            data[k.strip().upper()] = v.strip()

    required = ['NS', 'PRD', 'LOT']
    missing = [k for k in required if k not in data]
    if missing:
        raise ValueError(f"Faltan campos en QR: {', '.join(missing)}")

    try:
        return {
            'nro_serie': int(data['NS']),
            'id_producto': int(data['PRD']),
            'lote': data['LOT'],
        }
    except:
        raise ValueError("NS o PRD deben ser números válidos")


# =======================
# REGISTRAR BAJA EN productos_bajas
# =======================
def registrar_baja(conn, id_producto: int, lote: str, cantidad: int, motivo: str, observaciones: str = None, nro_serie: int = None):
    cfg = get_pg_config()
    schema = cfg["schema"]
    bajas_tbl = cfg["table_bajas"]

    with conn.cursor() as cur:
        cur.execute(f"""
            INSERT INTO {schema}.{bajas_tbl} (
                id_producto,
                stock_lote,
                fecha_hora,
                cantidad,
                motivo,
                observaciones
            ) VALUES (
                %s, %s, NOW(), %s, %s, %s
            ) RETURNING id;
        """, (id_producto, lote, cantidad, motivo, observaciones or None))
        return cur.fetchone()[0]


# =======================
# BAJA POR QR (con motivo y observaciones)
# =======================
def baja_por_qr(conn, id_producto: int, lote: str, nro_serie: int, raw_payload: str, motivo: str, observaciones: str = None):
    cfg = get_pg_config()
    schema = cfg["schema"]
    stock_tbl = cfg["table_stock"]
    bajas_tbl = cfg["table_bajas"]

    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT id, serie_inicio, serie_fin, packs_fin
            FROM {schema}.{stock_tbl}
            WHERE id_producto = %s AND lote = %s 
              AND %s BETWEEN serie_inicio AND serie_fin
            LIMIT 1;
        """, (id_producto, lote, nro_serie))
        row = cur.fetchone()
        if not row:
            raise ValueError(f"Serie {nro_serie} no encontrada")

        stock_id, inicio, fin, packs_fin = row
        packs_fin = int(packs_fin or 0)

        unit_type = 'packs' if packs_fin > 0 and nro_serie == fin else 'pallet'
        cantidad = packs_fin if unit_type == 'packs' else 1

        # Registrar en productos_bajas
        registrar_baja(conn, id_producto, lote, cantidad, motivo, observaciones, nro_serie)

        # Ajustar stock (igual que antes)
        if unit_type == 'packs':
            adjust_or_delete_range(conn, stock_id, new_packs_fin=0)
        else:
            new_fin = nro_serie - 1
            if new_fin < inicio:
                adjust_or_delete_range(conn, stock_id)
            else:
                adjust_or_delete_range(conn, stock_id, new_serie_fin=new_fin)

        update_ultimo_serie_por_lote(conn, id_producto, lote)

        return unit_type, cantidad


# =======================
# DROPDOWNS
# =======================
def get_products_with_stock(conn):
    cfg = get_pg_config()
    schema = cfg["schema"]

    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT DISTINCT p.id, p.descripcion
            FROM {schema}.productos p
            INNER JOIN {schema}.stock s ON p.id = s.id_producto
            ORDER BY p.id ASC;
        """)
        return cur.fetchall()


def get_lotes_for_product(conn, id_producto: int):
    cfg = get_pg_config()
    schema = cfg["schema"]

    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT DISTINCT s.lote
            FROM {schema}.stock s
            WHERE s.id_producto = %s
            ORDER BY s.lote ASC;
        """, (id_producto,))
        return [row[0] for row in cur.fetchall()]


def compute_net_stock(conn, id_producto: int, lote: str):
    cfg = get_pg_config()
    schema = cfg["schema"]
    stock_tbl = cfg["table_stock"]
    bajas_tbl = cfg["table_bajas"]
    prod_tbl = cfg["table_products"]

    with conn.cursor() as cur:
        cur.execute(f"SELECT descripcion FROM {schema}.{prod_tbl} WHERE id = %s;", (id_producto,))
        desc = cur.fetchone()[0].strip() if cur.rowcount > 0 else "Sin descripción"

        cur.execute(f"""
            SELECT 
                COALESCE(SUM((serie_fin - serie_inicio + 1) - CASE WHEN packs_fin > 0 THEN 1 ELSE 0 END), 0),
                COALESCE(SUM(packs_fin), 0)
            FROM {schema}.{stock_tbl}
            WHERE id_producto = %s AND lote = %s;
        """, (id_producto, lote))
        in_pallets, in_packs = cur.fetchone() or (0, 0)

        cur.execute(f"""
            SELECT 
                COALESCE(SUM(cantidad), 0)
            FROM {schema}.{bajas_tbl}
            WHERE id_producto = %s AND stock_lote = %s;
        """, (id_producto, lote))
        total_bajas = cur.fetchone()[0] or 0

    # Nota: si las bajas son por unidad, el neto es in - total_bajas
    # Ajusta según tu lógica real de cálculo
    net_total = in_pallets + in_packs - total_bajas  # simplificado, cámbialo si usas pallets/packs separados

    return net_total, desc


# =======================
# UI COMPLETA
# =======================
def main():
    try:
        conn = pg_connect()
        init_tables(conn)
    except Exception as e:
        messagebox.showerror("Error PostgreSQL", f"No se pudo conectar:\n{str(e)}")
        return

    root = tb.Window(themename="minty")
    root.title("Baja por QR / Manual – Talca")
    root.geometry("1100x800")

    tb.Label(root, text="Baja por QR o Manual", font=("Segoe UI", 22, "bold")).pack(pady=16)

    # Motivo (siempre vuelve a Venta)
    motivo_var = tb.StringVar(value="Venta")
    frame_motivo = tb.Frame(root)
    frame_motivo.pack(pady=8)
    tb.Label(frame_motivo, text="Motivo de baja:").pack(side="left", padx=10)
    tb.Radiobutton(frame_motivo, text="Venta", variable=motivo_var, value="Venta").pack(side="left", padx=10)
    tb.Radiobutton(frame_motivo, text="Calidad", variable=motivo_var, value="Calidad").pack(side="left", padx=10)
    tb.Radiobutton(frame_motivo, text="Desarme", variable=motivo_var, value="Desarme").pack(side="left", padx=10)

    # Observaciones (opcional)
    tb.Label(root, text="Observaciones (opcional):", font=("Segoe UI", 12)).pack(pady=4)
    obs_var = tb.StringVar()
    obs_entry = tb.Entry(root, textvariable=obs_var, width=90, font=("Segoe UI", 12))
    obs_entry.pack(pady=4)

    # Modo QR
    tb.Label(root, text="Modo QR: Escanea con pistola lectora", font=("Segoe UI", 14)).pack(pady=12)
    qr_var = tb.StringVar()
    qr_entry = tb.Entry(root, textvariable=qr_var, width=80, font=("Segoe UI", 14))
    qr_entry.pack(pady=8)
    qr_entry.focus_set()

    # Modo Manual
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
    cant_var = tb.StringVar()
    tb.Entry(frame_manual, textvariable=cant_var, width=12).grid(row=3, column=1, sticky="w", padx=12)

    btn_manual = tb.Button(root, text="EJECUTAR BAJA MANUAL", bootstyle=WARNING, width=25, command=lambda: on_manual_baja(motivo_var.get(), obs_var.get()))
    btn_manual.pack(pady=16)

    status_var = tb.StringVar(value="🟢 Listo – escanea QR o usa manual (motivo por default: Venta)")
    tb.Label(root, textvariable=status_var, font=("Segoe UI", 12), wraplength=900, justify="left").pack(pady=12, padx=40)

    def reset_form():
        motivo_var.set("Venta")
        obs_var.set("")

    def on_qr_scan(event=None):
        raw = qr_var.get().strip()
        if not raw:
            return
        qr_var.set("")
        qr_entry.focus_set()

        try:
            qr_data = parse_qr(raw)
            pid = qr_data['id_producto']
            lote = qr_data['lote']
            nserie = qr_data['nro_serie']

            motivo = motivo_var.get()
            obs = obs_var.get().strip() or None

            unit_type, cantidad = baja_por_qr(conn, pid, lote, nserie, raw, motivo, obs)

            net_p, net_pk, desc = compute_net_stock(conn, pid, lote)

            status_var.set(
                f"✅ Baja por QR registrada\n"
                f"Motivo: {motivo}\n"
                f"Observaciones: {obs if obs else 'Ninguna'}\n"
                f"Producto: {pid} - {desc}\n"
                f"Lote: {lote} | Serie: {nserie}\n"
                f"Tipo: {unit_type} | Cantidad: {cantidad}\n"
                f"Stock neto → Pallets: {net_p} | Packs: {net_pk}"
            )

            reset_form()

        except Exception as e:
            status_var.set(f"❌ ERROR en QR: {str(e)}")

    qr_entry.bind("<Return>", on_qr_scan)

    def on_manual_baja(motivo, obs):
        try:
            pstr = prod_var.get()
            if not pstr: raise ValueError("Selecciona producto")
            pid = int(pstr.split(" - ")[0])

            lote = lote_var.get().strip()
            if not lote: raise ValueError("Selecciona lote")

            tipo = type_var.get()
            qty_str = cant_var.get().strip()
            if not qty_str.isdigit(): raise ValueError("Cantidad debe ser número")
            qty = int(qty_str)
            if qty <= 0: raise ValueError("Cantidad > 0")

            # Registrar baja manual en productos_bajas
            registrar_baja(conn, pid, lote, qty, motivo, obs.strip() or None)

            # Ajustar stock (tu función existente)
            ids, new_serie, desc = eliminar_batch_ultimos(conn, pid, lote, tipo, qty)

            net_p, net_pk, _ = compute_net_stock(conn, pid, lote)

            status_var.set(
                f"✅ Baja manual registrada\n"
                f"Motivo: {motivo}\n"
                f"Observaciones: {obs if obs else 'Ninguna'}\n"
                f"Producto: {pid} - {desc}\n"
                f"Lote: {lote}\n"
                f"Ajustado: {qty} {tipo}\n"
                f"Nuevo último serie: {new_serie if new_serie is not None else 'Ninguno'}\n"
                f"Stock neto → Pallets: {net_p} | Packs: {net_pk}"
            )

            reset_form()
            cant_var.set("")

        except Exception as e:
            status_var.set(f"❌ ERROR manual: {str(e)}")

    def on_product_select(*args):
        val = prod_var.get()
        if not val:
            lote_combo['values'] = []
            return
        try:
            pid = int(val.split(" - ")[0])
            lotes = get_lotes_for_product(conn, pid)
            lote_combo['values'] = lotes
            lote_var.set(lotes[0] if lotes else "")
        except:
            lote_combo['values'] = []

    prod_combo.bind("<<ComboboxSelected>>", on_product_select)

    if options:
        prod_var.set(options[0])
        on_product_select()

    root.mainloop()


if __name__ == "__main__":
    main()