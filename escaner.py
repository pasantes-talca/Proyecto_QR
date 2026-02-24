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
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
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
#   LÓGICA DE STOCK (INGRESO - SALIDA)
# =======================
def compute_ingresos_totals(conn, id_producto: int, lote: str):
    """
    Ingresos:
      pallets = SUM( (serie_fin - serie_inicio + 1) - (packs_fin>0 ? 1 : 0) )
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
#   NUEVA LÓGICA PARA ELIMINAR / AJUSTAR ÚLTIMOS N PALLETS O PACKS EN stock_pp
# =======================
def get_last_ingreso_ranges(conn, id_producto: int, lote: str):
    """
    Obtiene los rangos de ingreso ordenados por serie_fin DESC (últimos primero).
    Devuelve lista de (id, serie_inicio, serie_fin, packs_fin)
    """
    cfg = get_pg_config()
    schema = cfg["schema"]
    tpp = cfg["table_pp"]

    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT id, serie_inicio, serie_fin, packs_fin
            FROM {schema}.{tpp}
            WHERE id_producto = %s AND lote = %s
            ORDER BY serie_fin DESC;
        """, (int(id_producto), str(lote)))
        return cur.fetchall()


def adjust_or_delete_range(conn, row_id: int, new_serie_fin: int = None, new_packs_fin: int = None):
    """
    Ajusta serie_fin o packs_fin de un rango, o elimina si se vacía.
    """
    cfg = get_pg_config()
    schema = cfg["schema"]
    tpp = cfg["table_pp"]

    with conn.cursor() as cur:
        if new_serie_fin is not None or new_packs_fin is not None:
            sets = []
            params = []
            if new_serie_fin is not None:
                sets.append("serie_fin = %s")
                params.append(int(new_serie_fin))
            if new_packs_fin is not None:
                sets.append("packs_fin = %s")
                params.append(int(new_packs_fin))
            params.append(int(row_id))
            cur.execute(f"""
                UPDATE {schema}.{tpp}
                SET {', '.join(sets)}
                WHERE id = %s;
            """, params)
        else:
            # Eliminar
            cur.execute(f"DELETE FROM {schema}.{tpp} WHERE id = %s;", (int(row_id),))


def check_if_range_has_salidas(conn, id_producto: int, lote: str, inicio: int, fin: int):
    """
    Verifica si hay salidas en el rango. Si hay, levanta error.
    """
    cfg = get_pg_config()
    schema = cfg["schema"]
    salidas = cfg["table_salidas"]

    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT COUNT(*)
            FROM {schema}.{salidas}
            WHERE id_producto = %s AND lote = %s AND nro_serie BETWEEN %s AND %s;
        """, (int(id_producto), str(lote), int(inicio), int(fin)))
        count = cur.fetchone()[0]
        if count > 0:
            raise ValueError(f"No se puede ajustar/eliminar rango con {count} salidas registradas.")


def eliminar_batch_ultimos(conn, id_producto: int, lote: str, unit_type: str, cantidad: int):
    """
    Elimina / ajusta los últimos 'cantidad' pallets o packs en stock_pp.
    - Para packs: reduce packs_fin del último rango.
    - Para pallets: reduce serie_fin del último rango, o elimina rangos completos.
    Devuelve el nuevo último serie (o None si vacío), y desc.
    Asume no hay salidas en los rangos a ajustar (verifica).
    """
    net_p, net_pk, desc = compute_net_stock(conn, id_producto, lote)
    if unit_type == 'pallet':
        if net_p < cantidad:
            raise ValueError(f"No hay suficientes pallets ingresados: {net_p} < {cantidad}")
    elif unit_type == 'packs':
        if net_pk < cantidad:
            raise ValueError(f"No hay suficientes packs ingresados: {net_pk} < {cantidad}")
    else:
        raise ValueError("unit_type debe ser 'pallet' o 'packs'")

    ranges = get_last_ingreso_ranges(conn, id_producto, lote)
    if not ranges:
        raise ValueError("No hay rangos de ingreso para este producto/lote.")

    remaining = cantidad
    adjusted_ids = []
    i = 0  # Índice en ranges (ya DESC)

    while remaining > 0 and i < len(ranges):
        row_id, inicio, fin, packs_fin = ranges[i]
        packs_fin = int(packs_fin or 0)
        check_if_range_has_salidas(conn, id_producto, lote, inicio, fin)

        if unit_type == 'packs':
            if packs_fin == 0:
                i += 1
                continue
            # Reducir packs_fin
            reduce_by = min(remaining, packs_fin)
            new_packs = packs_fin - reduce_by
            adjust_or_delete_range(conn, row_id, new_packs_fin=new_packs)
            adjusted_ids.append(row_id)
            remaining -= reduce_by
            if new_packs == 0:
                print(f"Eliminado rango {row_id} con packs_fin=0")
            # Si packs_fin=0, podría ajustar a pallet completo, pero por simplitud, lo dejamos en 0.
        elif unit_type == 'pallet':
            # Calcular pallets en este rango
            pallets_in_range = (fin - inicio + 1) - (1 if packs_fin > 0 else 0)
            if pallets_in_range <= 0:
                i += 1
                continue
            reduce_by = min(remaining, pallets_in_range)
            if reduce_by == pallets_in_range:
                # Eliminar todo el rango (pero preservar packs_fin si >0? No, si es pallet, ignoramos packs)
                adjust_or_delete_range(conn, row_id)
            else:
                # Reducir serie_fin
                new_fin = fin - reduce_by
                if packs_fin > 0 and new_fin < fin:
                    # Si había packs, pero reducimos pallets, packs quedan en el nuevo fin? No, packs están en fin, así que si reducimos, packs se "pierden" si no ajustamos.
                    # Asumimos que packs están asociados al último, así que si reducimos pallets, mantenemos packs_fin en el nuevo fin.
                    adjust_or_delete_range(conn, row_id, new_serie_fin=new_fin)
                else:
                    adjust_or_delete_range(conn, row_id, new_serie_fin=new_fin)
            adjusted_ids.append(row_id)
            remaining -= reduce_by
        i += 1

    if remaining > 0:
        raise ValueError(f"No se pudo eliminar toda la cantidad: faltan {remaining}")

    # Obtener nuevo último serie_fin
    new_ranges = get_last_ingreso_ranges(conn, id_producto, lote)
    new_last_serie = new_ranges[0][2] if new_ranges else None  # serie_fin del último

    # Actualizar Sheets (con nuevo neto)
    payload = build_payload_for_product_lote_net(conn, id_producto, lote)
    queue_outbox(conn, payload)

    return adjusted_ids, new_last_serie, desc


# =======================
#   FUNCIONES PARA DROPDOWNS
# =======================
def get_products_with_stock(conn):
    """
    Obtiene lista de (id_producto, descripcion) que tienen entradas en stock_pp.
    Ordenados por id_producto ASC.
    """
    cfg = get_pg_config()
    schema = cfg["schema"]
    tpp = cfg["table_pp"]
    prod = cfg["table_products"]

    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT DISTINCT p.id_producto, p.descripcion
            FROM {schema}.{prod} p
            INNER JOIN {schema}.{tpp} pp ON p.id_producto = pp.id_producto
            ORDER BY p.id_producto ASC;
        """)
        return cur.fetchall()  # lista de (id, desc)


def get_lotes_for_product(conn, id_producto: int):
    """
    Obtiene lista de lotes distintos para un producto, ordenados ASC.
    """
    cfg = get_pg_config()
    schema = cfg["schema"]
    tpp = cfg["table_pp"]

    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT DISTINCT lote
            FROM {schema}.{tpp}
            WHERE id_producto = %s
            ORDER BY lote ASC;
        """, (int(id_producto),))
        return [row[0] for row in cur.fetchall()]


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
    root.title("Eliminar / Ajustar Últimos Ingresos – Talca (PostgreSQL)")
    root.geometry("920x560")

    tb.Label(root, text="Eliminar / Ajustar Últimos Pallets / Packs", font=("Segoe UI", 20, "bold")).pack(pady=14)
    tb.Label(
        root,
        text="Selecciona producto y lote desde BD, tipo y cantidad para eliminar/ajustar los últimos N en ingresos (stock_pp).\n"
             "Actualiza serie_fin / packs_fin, y stock neto. Verifica no haya salidas en los rangos.",
        font=("Segoe UI", 10)
    ).pack(pady=6)

    # Modo Batch
    tb.Label(root, text="Ajuste Últimos:", font=("Segoe UI", 12, "bold")).pack(pady=8)
    frame_batch = tb.Frame(root)
    frame_batch.pack(pady=4)

    # Dropdown Productos
    tb.Label(frame_batch, text="Producto:").grid(row=0, column=0, padx=4)
    products = get_products_with_stock(conn_pg)
    product_options = [f"{pid} - {desc}" for pid, desc in products]
    prod_var = tb.StringVar()
    prod_combo = tb.Combobox(frame_batch, textvariable=prod_var, values=product_options, width=50)
    prod_combo.grid(row=0, column=1, columnspan=3, padx=4)

    # Dropdown Lotes (inicial vacío)
    tb.Label(frame_batch, text="Lote:").grid(row=1, column=0, padx=4, pady=4)
    lote_var = tb.StringVar()
    lote_combo = tb.Combobox(frame_batch, textvariable=lote_var, values=[], width=15)
    lote_combo.grid(row=1, column=1, padx=4)

    # Tipo
    tb.Label(frame_batch, text="Tipo:").grid(row=1, column=2, padx=4)
    type_var = tb.StringVar(value="pallet")
    tb.Radiobutton(frame_batch, text="Pallets", variable=type_var, value="pallet").grid(row=1, column=3)
    tb.Radiobutton(frame_batch, text="Packs", variable=type_var, value="packs").grid(row=1, column=4)

    # Cantidad
    tb.Label(frame_batch, text="Cantidad:").grid(row=2, column=0, padx=4, pady=4)
    cant_var = tb.StringVar()
    entry_cant = tb.Entry(frame_batch, textvariable=cant_var, width=10)
    entry_cant.grid(row=2, column=1, padx=4)

    btn_batch = tb.Button(root, text="Eliminar / Ajustar Batch", bootstyle=DANGER, command=lambda: on_batch_eliminar())
    btn_batch.pack(pady=8)

    status_var = tb.StringVar(value="🟢 Listo: selecciona producto, lote, tipo, cantidad y confirma.")
    tb.Label(root, textvariable=status_var, font=("Segoe UI", 11), justify="left").pack(pady=10)

    sheets_var = tb.StringVar(value=f"Sheets pendientes (outbox): {outbox_count(conn_pg)}")
    tb.Label(root, textvariable=sheets_var, font=("Segoe UI", 10)).pack(pady=4)

    # Evento para actualizar lotes al seleccionar producto
    def on_product_select(event=None):
        selected = prod_var.get()
        if not selected:
            lote_combo['values'] = []
            return
        try:
            pid = int(selected.split(" - ", 1)[0])
            lotes = get_lotes_for_product(conn_pg, pid)
            lote_combo['values'] = lotes
            if lotes:
                lote_var.set(lotes[0])
            else:
                lote_var.set("")
        except Exception:
            lote_combo['values'] = []

    prod_combo.bind("<<ComboboxSelected>>", on_product_select)

    def on_batch_eliminar():
        try:
            selected_prod = prod_var.get()
            if not selected_prod:
                raise ValueError("Selecciona un producto.")
            pid = int(selected_prod.split(" - ", 1)[0])

            lote = lote_var.get().strip()
            if not lote:
                raise ValueError("Selecciona un lote.")

            unit_type = type_var.get()
            cantidad = int(cant_var.get().strip())
            if cantidad <= 0:
                raise ValueError("Cantidad debe ser positiva.")

            net_p_before, net_pk_before, desc = compute_net_stock(conn_pg, pid, lote)

            adjusted_ids, new_last_serie, desc = eliminar_batch_ultimos(conn_pg, pid, lote, unit_type, cantidad)

            net_p_after, net_pk_after, _ = compute_net_stock(conn_pg, pid, lote)

            try:
                sent = flush_outbox(conn_pg)
                pending = outbox_count(conn_pg)
                sheets_var.set(f"✅ Enviado(s): {sent} | Pendientes (outbox): {pending}")
            except Exception as e:
                pending = outbox_count(conn_pg)
                sheets_var.set(f"⚠️ No se pudo enviar a Sheets: {e} | Pendientes (outbox): {pending}")

            root.bell()

            new_serie_txt = f"Nuevo último serie: {new_last_serie}" if new_last_serie else "No quedan series"

            status_var.set(
                f"✅ Ajuste batch realizado\n"
                f"IDs ajustados/eliminados: {', '.join(map(str, adjusted_ids))}\n"
                f"Producto: {pid} | {desc}\n"
                f"Lote: {lote}\n"
                f"Se eliminó/ajustó: {cantidad} {unit_type}\n"
                f"{new_serie_txt}\n\n"
                f"📦 Stock neto pallets: {net_p_after} (antes {net_p_before})\n"
                f"📦 Stock neto packs: {net_pk_after} (antes {net_pk_before})"
            )

            # Limpiar cantidad
            cant_var.set("")
            entry_cant.focus_set()

        except Exception as e:
            status_var.set(f"❌ ERROR en ajuste: {e}")
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
    tb.Button(btn_frame, text="Limpiar", bootstyle=SECONDARY, command=lambda: status_var.set("🟢 Listo: selecciona producto, lote, tipo, cantidad y confirma.")).pack(side="left", padx=6)

    entry_cant.bind("<Return>", lambda e: on_batch_eliminar())
    lote_combo.bind("<Return>", lambda e: entry_cant.focus_set())

    # Preseleccionar primer producto si hay
    if product_options:
        prod_var.set(product_options[0])
        on_product_select()

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