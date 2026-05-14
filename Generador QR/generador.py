import os
import json
import io
import textwrap
from datetime import datetime
from dateutil.relativedelta import relativedelta

from flask import Flask, render_template, request, send_file, flash, redirect, url_for
import qrcode
from reportlab.lib.pagesizes import A4
from reportlab.pdfgen import canvas
from reportlab.lib.utils import ImageReader
import psycopg2

app = Flask(__name__)
app.secret_key = "clave_super_secreta_para_flash_messages" # Cambiá esto en producción

# =======================
#   CONFIGURACIÓN Y BD
# =======================
CONFIG_FILE = "config.json"

DEFAULT_PG = {
    "host":"10.242.4.13",
    "port": 5432,
    "dbname": "stock",
    "user": "postgres",
    "password": "Talca2025",
    "client_encoding": "WIN1252",
    "schema": "produccion",
    "table_products": "productos",
    "table_stock": "stock"
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
            if v:
                cfg[k] = v
    cfg["port"] = int(cfg.get("port", 5432))
    return cfg

def pg_connect():
    cfg = get_pg_config()
    conn = psycopg2.connect(
        host=cfg["host"], port=cfg["port"], dbname=cfg["dbname"],
        user=cfg["user"], password=cfg["password"]
    )
    conn.autocommit = True
    if cfg.get("client_encoding"):
        conn.set_client_encoding(cfg["client_encoding"])
    return conn

def fetch_products(conn):
    cfg = get_pg_config()
    schema, prod = cfg["schema"], cfg["table_products"]
    with conn.cursor() as cur:
        cur.execute(f"SELECT id, descripcion FROM {schema}.{prod} ORDER BY descripcion ASC;")
        return cur.fetchall()

def get_db_max_serie_for_producto(conn, id_producto: int) -> int:
    cfg = get_pg_config()
    schema, tstock = cfg["schema"], cfg["table_stock"]
    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT COALESCE(MAX(nro_serie), 0)
            FROM {schema}.{tstock}
            WHERE id_producto = %s;
        """, (int(id_producto),))
        return int(cur.fetchone()[0] or 0)

def get_starting_serie(conn, id_producto: int, lote: str) -> int:
    db_max = get_db_max_serie_for_producto(conn, id_producto)
    data = load_config()
    cache_key = f"gen_ultimo_serie::{id_producto}::{lote}"
    cache_max = int(data.get("cache", {}).get(cache_key, 0) or 0)
    return max(db_max, cache_max)

def set_last_generated(id_producto: int, lote: str, ultimo: int):
    data = load_config()
    if not isinstance(data.get("cache"), dict):
        data["cache"] = {}
    data["cache"][f"gen_ultimo_serie::{id_producto}::{lote}"] = int(ultimo)
    save_config(data)

def dividir_texto(texto, max_caracteres):
    return textwrap.wrap(str(texto), width=max_caracteres)

# =======================
#   GENERACIÓN PDF EN MEMORIA
# =======================
def generar_pdf_en_memoria(conn, id_producto: int, descripcion: str, cantidad: int):
    fecha_actual = datetime.now()
    numero_lote = fecha_actual.strftime("%d%m%y")
    fec_iso = fecha_actual.strftime("%Y-%m-%d")
    vto_iso = (fecha_actual + relativedelta(months=6)).strftime("%Y-%m-%d")
    fecha_str = fecha_actual.strftime("%d/%m/%y")
    fecha_venc_str = (fecha_actual + relativedelta(months=6)).strftime("%d/%m/%y")

    nro_serie = get_starting_serie(conn, id_producto, numero_lote)

    # Creamos un buffer en memoria en vez de un archivo en disco
    pdf_buffer = io.BytesIO()
    c = canvas.Canvas(pdf_buffer, pagesize=A4)
    _, alto = A4

    y_positions = [alto - 230, alto - 430, alto - 630, alto - 830]
    x_qr, qr_size = 40, 215
    text_x = x_qr + qr_size + 40
    posicion_actual = 0

    desc_clean = str(descripcion).replace("\n", " ").replace("|", "/").replace("=", "-").strip()
    if len(desc_clean) > 90: desc_clean = desc_clean[:90]

    for _ in range(cantidad):
        nro_serie += 1
        payload_qr = (f"NS={nro_serie:06d}|PRD={id_producto}|DSC={desc_clean}"
                      f"|LOT={numero_lote}|FEC={fec_iso}|VTO={vto_iso}")

        # Generar QR en memoria
        qr_img = qrcode.make(payload_qr)
        img_buffer = io.BytesIO()
        qr_img.save(img_buffer, format="PNG")
        img_buffer.seek(0)
        qr_reader = ImageReader(img_buffer)

        for _ in range(2): # 2 copias por QR
            y = y_positions[posicion_actual]
            c.drawImage(qr_reader, x_qr, y, width=qr_size, height=qr_size)

            titulo_lineas = dividir_texto(descripcion, 40)
            resto_lineas = [
                f"N° de serie: {nro_serie}", f"ID producto: {id_producto}",
                f"Lote: {numero_lote}", f"Creación: {fecha_str}", f"Vencimiento: {fecha_venc_str}",
            ]

            titulo_height = len(titulo_lineas) * 18
            resto_height = len(resto_lineas) * 15
            text_y = (y + qr_size / 2) + (titulo_height + resto_height) / 2

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

    c.save()
    pdf_buffer.seek(0)
    set_last_generated(id_producto, numero_lote, nro_serie)
    
    return pdf_buffer, numero_lote, nro_serie

# =======================
#   RUTAS FLASK
# =======================
@app.route("/", methods=["GET"])
def index():
    try:
        conn = pg_connect()
        productos = fetch_products(conn)
        conn.close()
    except Exception as e:
        flash(f"Error de base de datos: {e}", "danger")
        productos = []

    return render_template("index.html", productos=productos)

@app.route("/generar", methods=["POST"])
def generar():
    producto_data = request.form.get("producto")
    cantidad_str = request.form.get("cantidad")

    if not producto_data or not cantidad_str.isdigit() or int(cantidad_str) <= 0:
        flash("Por favor, seleccioná un producto y una cantidad válida.", "warning")
        return redirect(url_for("index"))

    id_producto, descripcion = producto_data.split("|", 1)
    id_producto = int(id_producto)
    cantidad = int(cantidad_str)

    try:
        conn = pg_connect()
        pdf_buffer, lote, ultimo_serie = generar_pdf_en_memoria(conn, id_producto, descripcion, cantidad)
        conn.close()
        
        filename = f"qr_lote_{lote}_prd_{id_producto}.pdf"
        
        # Enviamos el PDF directamente al navegador
        return send_file(
            pdf_buffer,
            as_attachment=True,
            download_name=filename,
            mimetype="application/pdf"
        )
    except Exception as e:
        flash(f"Error generando el PDF: {e}", "danger")
        return redirect(url_for("index"))

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)