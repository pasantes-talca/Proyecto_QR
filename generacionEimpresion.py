import os
import sys
import json
import textwrap
from datetime import datetime
from dateutil.relativedelta import relativedelta

import qrcode
from reportlab.lib.pagesizes import A4
from reportlab.pdfgen import canvas

import ttkbootstrap as tb
from ttkbootstrap.constants import *
from tkinter import messagebox, filedialog


# ----------------------------
#   POSTGRES DRIVER
# ----------------------------
try:
    import psycopg2
except Exception:
    psycopg2 = None


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
}


# =======================
#   CACHE
# =======================
def load_cache():
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except:
            return {}
    return {}


def save_cache(data: dict):
    try:
        with open(CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except:
        pass


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


def fetch_products(conn):
    cfg = get_pg_config()
    schema = cfg["schema"]
    prod = cfg["table_products"]

    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT id_producto, descripcion
            FROM {schema}.{prod}
            ORDER BY descripcion ASC;
        """)
        return cur.fetchall()


def get_product_row(conn, id_producto: int):
    cfg = get_pg_config()
    schema = cfg["schema"]
    prod = cfg["table_products"]

    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT id_producto, descripcion, ultimo_nro_serie
            FROM {schema}.{prod}
            WHERE id_producto = %s;
        """, (int(id_producto),))
        return cur.fetchone()


def update_ultimo_nro_serie(conn, id_producto: int, ultimo: int):
    cfg = get_pg_config()
    schema = cfg["schema"]
    prod = cfg["table_products"]

    with conn.cursor() as cur:
        cur.execute(f"""
            UPDATE {schema}.{prod}
            SET ultimo_nro_serie = %s
            WHERE id_producto = %s;
        """, (int(ultimo), int(id_producto)))


# =======================
#   PDF QRS
# =======================
def dividir_texto(texto, max_caracteres):
    return textwrap.wrap(str(texto), width=max_caracteres)


def generar_y_imprimir_qrs(conn, id_producto: int, descripcion: str, cantidad: int):
    row = get_product_row(conn, id_producto)
    if not row:
        messagebox.showerror("Error", "Producto no encontrado en Postgres.")
        return

    _, _, ultimo = row
    nro_serie = int(ultimo or 0)

    fecha_actual = datetime.now()
    fec_iso = fecha_actual.strftime("%Y-%m-%d")
    vto_iso = (fecha_actual + relativedelta(months=6)).strftime("%Y-%m-%d")

    fecha_str = fecha_actual.strftime("%d/%m/%y")
    fecha_venc_str = (fecha_actual + relativedelta(months=6)).strftime("%d/%m/%y")

    numero_lote = fecha_actual.strftime("%d%m%y")

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
        qr_path = os.path.join(APP_DIR, f"temp_qr_{id_producto}_{nro_serie}.png")
        qr.save(qr_path)

        # 2 copias por hoja (tal cual tu lógica)
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
        except:
            pass

    c.save()

    # actualizar ultimo_nro_serie en Postgres
    update_ultimo_nro_serie(conn, id_producto, nro_serie)

    messagebox.showinfo("PDF generado", f"El archivo se guardó correctamente:\n{pdf_path}")


# =======================
#   UI APP
# =======================
def main():
    # Conexión PG
    try:
        conn_pg = pg_connect()
    except Exception as e:
        messagebox.showerror(
            "PostgreSQL",
            "No pude conectar a PostgreSQL.\n\n"
            f"Error: {e}\n\n"
            "Tip: revisá host/puerto/db/user/pass en config.json (sección pg).\n"
            "Si usás client_encoding, asegurate que sea válido (ej: WIN1252)."
        )
        return

    # Carga productos
    try:
        productos = fetch_products(conn_pg)
    except Exception as e:
        messagebox.showerror("PostgreSQL", f"No pude leer stock.productos.\n\n{e}")
        return

    # UI
    root = tb.Window(themename="minty")
    root.title("Generación e Impresión de QRs – Talca (PostgreSQL)")
    root.geometry("900x520")

    tb.Label(root, text="Generador de QRs", font=("Segoe UI", 20, "bold")).pack(pady=18)
    tb.Label(root, text="Seleccioná un producto:", font=("Segoe UI", 12)).pack(pady=6)

    producto_dict = {f"{desc} (ID: {pid})": (int(pid), str(desc)) for pid, desc in productos}

    combo = tb.Combobox(root, values=list(producto_dict.keys()), width=90)
    combo.pack(pady=4)

    tb.Label(root, text="Cantidad de números de serie:", font=("Segoe UI", 12)).pack(pady=12)
    cantidad_entry = tb.Entry(root, width=12)
    cantidad_entry.pack()

    cache = load_cache()
    if cache.get("gen_producto") in producto_dict:
        combo.set(cache.get("gen_producto"))
    if cache.get("gen_cantidad"):
        try:
            cantidad_entry.insert(0, str(int(cache.get("gen_cantidad"))))
        except:
            pass

    def al_hacer_click_generar():
        if not combo.get():
            messagebox.showwarning("Aviso", "Seleccioná un producto.")
            return

        try:
            cantidad = int(cantidad_entry.get())
            if cantidad <= 0:
                raise ValueError
        except:
            messagebox.showwarning("Aviso", "Cantidad inválida.")
            return

        pid, desc = producto_dict[combo.get()]
        generar_y_imprimir_qrs(conn_pg, pid, desc, cantidad)

        cache2 = load_cache()
        cache2["gen_producto"] = combo.get()
        cache2["gen_cantidad"] = cantidad
        save_cache(cache2)

    tb.Button(root, text="GENERAR", bootstyle=SUCCESS, command=al_hacer_click_generar).pack(pady=22)

    def on_close():
        try:
            conn_pg.close()
        except:
            pass
        root.destroy()

    root.protocol("WM_DELETE_WINDOW", on_close)
    root.mainloop()


if __name__ == "__main__":
    main()
