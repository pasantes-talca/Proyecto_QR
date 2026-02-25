# generacionEimpresion.py
import os
import sys
import json
import textwrap
from datetime import datetime

try:
    from dateutil.relativedelta import relativedelta
except ModuleNotFoundError:
    raise SystemExit(
        "Falta el paquete 'python-dateutil'.\n"
        "Instalalo en tu venv con:\n"
        "  pip install python-dateutil\n"
    )

import qrcode
from reportlab.lib.pagesizes import A4
from reportlab.pdfgen import canvas

import ttkbootstrap as tb
from ttkbootstrap.constants import *
from tkinter import messagebox, filedialog

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
    "client_encoding": os.getenv("TALCA_PG_ENCODING", ""),  # ej: WIN1252
    "schema": "produccion",
    "table_products": "productos",
    "table_stock": "stock",
}


def load_config():
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
            return data if isinstance(data, dict) else {}
        except Exception:
            return {}
    return {}


def save_config(data: dict):
    try:
        with open(CONFIG_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception:
        pass


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


def cache_get(key: str, default=None):
    data = load_config()
    cache = data.get("cache", {})
    if isinstance(cache, dict) and key in cache:
        return cache.get(key, default)
    return default


def cache_set(key: str, value):
    data = load_config()
    if not isinstance(data.get("cache"), dict):
        data["cache"] = {}
    data["cache"][key] = value
    save_config(data)


# =======================
#   POSTGRES
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


def fetch_products(conn):
    """
    ✅ NUEVA BD:
      produccion.productos(id, descripcion)
    """
    cfg = get_pg_config()
    schema = cfg["schema"]
    prod = cfg["table_products"]

    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT id, descripcion
            FROM {schema}.{prod}
            ORDER BY descripcion ASC;
        """)
        return cur.fetchall()  # [(id, descripcion), ...]


def get_db_max_serie_for_lote(conn, id_producto: int, lote: str) -> int:
    """
    Busca el máximo nro_serie ya ingresado en produccion.stock para ese producto+lote.
    Si todavía no hay ingresos, devuelve 0.
    """
    cfg = get_pg_config()
    schema = cfg["schema"]
    tstock = cfg["table_stock"]

    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT COALESCE(MAX(nro_serie), 0)
            FROM {schema}.{tstock}
            WHERE id_producto = %s AND lote = %s;
        """, (int(id_producto), str(lote)))
        return int(cur.fetchone()[0] or 0)


def get_starting_serie(conn, id_producto: int, lote: str) -> int:
    """
    Evita duplicados:
    - max en DB (lo ya escaneado/ingresado)
    - max en cache local (lo ya generado en esta PC aunque todavía no escaneado)

    Arranca desde el mayor de ambos.
    """
    db_max = get_db_max_serie_for_lote(conn, id_producto, lote)
    cache_key = f"gen_ultimo_serie::{id_producto}::{lote}"
    cache_max = int(cache_get(cache_key, 0) or 0)
    return max(db_max, cache_max)


def set_last_generated(id_producto: int, lote: str, ultimo: int):
    cache_key = f"gen_ultimo_serie::{id_producto}::{lote}"
    cache_set(cache_key, int(ultimo))


# =======================
#   PDF / QR
# =======================
def dividir_texto(texto, max_caracteres):
    return textwrap.wrap(str(texto), width=max_caracteres)


def generar_y_imprimir_qrs(conn, id_producto: int, descripcion: str, cantidad: int):
    fecha_actual = datetime.now()

    # Lote = ddmmyy (igual que antes)
    numero_lote = fecha_actual.strftime("%d%m%y")

    fec_iso = fecha_actual.strftime("%Y-%m-%d")
    vto_iso = (fecha_actual + relativedelta(months=6)).strftime("%Y-%m-%d")

    fecha_str = fecha_actual.strftime("%d/%m/%y")
    fecha_venc_str = (fecha_actual + relativedelta(months=6)).strftime("%d/%m/%y")

    # Serie inicial (DB vs cache)
    nro_serie = get_starting_serie(conn, id_producto, numero_lote)

    pdf_path = filedialog.asksaveasfilename(
        defaultextension=".pdf",
        filetypes=[("PDF", "*.pdf")],
        initialfile=f"qr_lote_{numero_lote}.pdf"
    )
    if not pdf_path:
        return

    c = canvas.Canvas(pdf_path, pagesize=A4)
    _, alto = A4

    y_positions = [alto - 230, alto - 430, alto - 630, alto - 830]
    x_qr = 40
    qr_size = 215
    text_x = x_qr + qr_size + 40
    posicion_actual = 0

    desc_clean = str(descripcion).replace("\n", " ").replace("|", "/").replace("=", "-").strip()
    if len(desc_clean) > 90:
        desc_clean = desc_clean[:90]

    for _ in range(cantidad):
        nro_serie += 1

        payload_qr = (
            f"NS={nro_serie:06d}"
            f"|PRD={id_producto}"
            f"|DSC={desc_clean}"
            f"|LOT={numero_lote}"
            f"|FEC={fec_iso}"
            f"|VTO={vto_iso}"
        )

        qr = qrcode.make(payload_qr)
        qr_path = os.path.join(APP_DIR, f"temp_qr_{id_producto}_{numero_lote}_{nro_serie}.png")
        qr.save(qr_path)

        # 2 copias por QR
        for _ in range(2):
            y = y_positions[posicion_actual]
            c.drawImage(qr_path, x_qr, y, width=qr_size, height=qr_size)

            titulo_lineas = dividir_texto(descripcion, 40)
            resto_lineas = [
                f"N° de serie: {nro_serie}",
                f"ID producto: {id_producto}",
                f"Lote: {numero_lote}",
                f"Creación: {fecha_str}",
                f"Vencimiento: {fecha_venc_str}",
            ]

            titulo_height = len(titulo_lineas) * 18
            resto_height = len(resto_lineas) * 15
            total_height = titulo_height + resto_height

            centro_qr_y = y + qr_size / 2
            text_y = centro_qr_y + total_height / 2

            c.setFont("Helvetica-Bold", 15)
            for i, linea_txt in enumerate(titulo_lineas):
                c.drawString(text_x, text_y - i * 20, linea_txt)

            offset = titulo_height

            c.setFont("Helvetica-Bold", 18)
            c.drawString(text_x, text_y - offset, resto_lineas[0])
            offset += 20

            c.setFont("Helvetica", 15)
            for linea_txt in resto_lineas[1:]:
                c.drawString(text_x, text_y - offset, linea_txt)
                offset += 15

            posicion_actual += 1
            if posicion_actual == 4:
                c.showPage()
                posicion_actual = 0

        try:
            os.remove(qr_path)
        except Exception:
            pass

    c.save()

    # guardamos en cache el último generado (por producto + lote)
    set_last_generated(id_producto, numero_lote, nro_serie)

    messagebox.showinfo("PDF generado", f"✅ PDF guardado:\n{pdf_path}\n\nLote: {numero_lote}")


# =======================
#   UI
# =======================
def main():
    try:
        conn_pg = pg_connect()
    except Exception as e:
        messagebox.showerror("PostgreSQL", f"No pude conectar a PostgreSQL:\n\n{e}")
        return

    try:
        productos = fetch_products(conn_pg)
    except Exception as e:
        messagebox.showerror("PostgreSQL", f"No pude leer produccion.productos:\n\n{e}")
        return

    root = tb.Window(themename="minty")
    root.title("Generación e Impresión de QRs – Talca (PostgreSQL)")
    root.geometry("920x540")

    tb.Label(root, text="Generador de QRs", font=("Segoe UI", 20, "bold")).pack(pady=18)
    tb.Label(root, text="Seleccioná un producto:", font=("Segoe UI", 12)).pack(pady=6)

    producto_dict = {f"{desc} (ID: {pid})": (int(pid), str(desc)) for pid, desc in productos}

    combo = tb.Combobox(root, values=list(producto_dict.keys()), width=92)
    combo.pack(pady=4)

    tb.Label(root, text="Cantidad de números de serie:", font=("Segoe UI", 12)).pack(pady=12)
    cantidad_entry = tb.Entry(root, width=12)
    cantidad_entry.pack()

    # restaurar UI
    last_prod = cache_get("ui_gen_producto", "")
    last_cant = cache_get("ui_gen_cantidad", "")

    if last_prod in producto_dict:
        combo.set(last_prod)
    if last_cant:
        try:
            cantidad_entry.insert(0, str(int(last_cant)))
        except Exception:
            pass

    def al_hacer_click_generar():
        if not combo.get():
            messagebox.showwarning("Aviso", "Seleccioná un producto.")
            return

        try:
            cantidad = int(cantidad_entry.get())
            if cantidad <= 0:
                raise ValueError
        except Exception:
            messagebox.showwarning("Aviso", "Cantidad inválida.")
            return

        pid, desc = producto_dict[combo.get()]
        generar_y_imprimir_qrs(conn_pg, pid, desc, cantidad)

        cache_set("ui_gen_producto", combo.get())
        cache_set("ui_gen_cantidad", cantidad)

    tb.Button(root, text="GENERAR", bootstyle=SUCCESS, command=al_hacer_click_generar).pack(pady=22)

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