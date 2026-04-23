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
    "host": "10.242.4.13",
    "port": 5432,
    "dbname": "stock",
    "user": "postgres",
    "password": "Talca2025",
    "client_encoding": "WIN1252",
    "schema": "produccion",
    "table_products": "productos",
    "table_stock": "stock",
    "table_bajas": "bajas",
    "table_sheet": "sheet",
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


# =======================
#   CACHÉ LOCAL (por producto)
# =======================
def cache_key_for_product(id_producto: int) -> str:
    """Clave única de caché por producto (sin lote)."""
    return f"last_serie::{id_producto}"


def cache_get_serie(id_producto: int) -> int:
    """Devuelve el último N° de serie generado para este producto (0 si nunca se generó)."""
    data = load_config()
    cache = data.get("cache", {})
    key = cache_key_for_product(id_producto)
    try:
        return int(cache.get(key, 0))
    except Exception:
        return 0


def cache_set_serie(id_producto: int, ultimo: int):
    """Guarda el último N° de serie generado para este producto."""
    data = load_config()
    if not isinstance(data.get("cache"), dict):
        data["cache"] = {}
    data["cache"][cache_key_for_product(id_producto)] = int(ultimo)
    save_config(data)


def cache_reset_serie(id_producto: int):
    """Resetea a 0 el N° de serie del producto (próxima impresión arrancará desde 1)."""
    cache_set_serie(id_producto, 0)


def cache_get_ui(key: str, default=None):
    """Caché genérica para guardar estado de la UI (último producto seleccionado, etc.)."""
    data = load_config()
    return data.get("cache", {}).get(key, default)


def cache_set_ui(key: str, value):
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


# =======================
#   PDF / QR
# =======================
def dividir_texto(texto, max_caracteres):
    return textwrap.wrap(str(texto), width=max_caracteres)


def generar_y_imprimir_qrs(id_producto: int, descripcion: str, cantidad: int, on_done_callback=None):
    """
    Genera el PDF con los QR codes.
    La serie SIEMPRE arranca desde la caché local del producto (0 si nunca se imprimió
    o fue reseteada → primer QR impreso tendrá N° de serie: 1).
    NO consulta la base de datos para el número de serie.
    """
    fecha_actual = datetime.now()
    numero_lote = fecha_actual.strftime("%d%m%y")
    fec_iso = fecha_actual.strftime("%Y-%m-%d")
    vto_iso = (fecha_actual + relativedelta(months=6)).strftime("%Y-%m-%d")
    fecha_str = fecha_actual.strftime("%d/%m/%y")
    fecha_venc_str = (fecha_actual + relativedelta(months=6)).strftime("%d/%m/%y")

    # ── Serie: sólo caché local ──────────────────────────────────────────────
    nro_serie = cache_get_serie(id_producto)
    # Si la caché es 0 (nuevo producto o reseteado), el primer QR será el 1.

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

    # ── Guardar último número generado en caché local ────────────────────────
    cache_set_serie(id_producto, nro_serie)

    # ── Abrir el PDF automáticamente ─────────────────────────────────────────
    try:
        if sys.platform == "win32":
            os.startfile(pdf_path)
        elif sys.platform == "darwin":
            os.system(f'open "{pdf_path}"')
        else:
            os.system(f'xdg-open "{pdf_path}"')
    except Exception as e:
        print(f"No se pudo abrir el PDF automáticamente: {e}")

    messagebox.showinfo(
        "PDF generado",
        f"✅ PDF guardado y abierto:\n{pdf_path}\n\n"
        f"Lote: {numero_lote}\n"
        f"Último número de serie generado: {nro_serie}"
    )

    # Notificar a la UI para que actualice el label de caché
    if on_done_callback:
        on_done_callback(nro_serie)


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
    root.geometry("960x580")

    # ── Título ────────────────────────────────────────────────────────────────
    tb.Label(
        root,
        text="Generador de QRs",
        font=("Segoe UI", 20, "bold")
    ).pack(pady=18)

    # ── Combo de productos ────────────────────────────────────────────────────
    tb.Label(root, text="Seleccioná un producto:", font=("Segoe UI", 12)).pack(pady=4)

    producto_dict = {
        f"{desc} (ID: {pid})": (int(pid), str(desc))
        for pid, desc in productos
    }

    combo = tb.Combobox(root, values=list(producto_dict.keys()), width=92, state="readonly")
    combo.pack(pady=4)

    # ── Panel de caché ────────────────────────────────────────────────────────
    frame_cache = tb.Labelframe(
        root,
        text="Estado de caché (último N° de serie impreso)",
        padding=12,
        bootstyle="info"
    )
    frame_cache.pack(fill="x", padx=40, pady=10)

    cache_label = tb.Label(
        frame_cache,
        text="— Seleccioná un producto para ver el estado —",
        font=("Segoe UI", 12),
        bootstyle="info"
    )
    cache_label.pack(side="left", padx=10)

    def actualizar_label_cache(pid: int | None = None):
        """Refresca el label que muestra el último N° de serie en caché."""
        if pid is None:
            cache_label.config(
                text="— Seleccioná un producto para ver el estado —",
                bootstyle="info"
            )
            return
        ultimo = cache_get_serie(pid)
        if ultimo == 0:
            cache_label.config(
                text="Sin impresiones previas → arrancará desde N° de serie: 1",
                bootstyle="success"
            )
        else:
            cache_label.config(
                text=f"Último N° de serie impreso: {ultimo}  →  el próximo será: {ultimo + 1}",
                bootstyle="warning"
            )

    def on_combo_change(event=None):
        sel = combo.get()
        if sel in producto_dict:
            pid, _ = producto_dict[sel]
            actualizar_label_cache(pid)
        else:
            actualizar_label_cache(None)

    combo.bind("<<ComboboxSelected>>", on_combo_change)

    # ── Cantidad ──────────────────────────────────────────────────────────────
    tb.Label(root, text="Cantidad de números de serie:", font=("Segoe UI", 12)).pack(pady=10)
    cantidad_entry = tb.Entry(root, width=12, font=("Segoe UI", 13))
    cantidad_entry.pack()

    # ── Restaurar estado de UI ────────────────────────────────────────────────
    last_prod = cache_get_ui("ui_gen_producto", "")
    last_cant = cache_get_ui("ui_gen_cantidad", "")

    if last_prod in producto_dict:
        combo.set(last_prod)
        on_combo_change()
    if last_cant:
        try:
            cantidad_entry.insert(0, str(int(last_cant)))
        except Exception:
            pass

    # ── Botones ───────────────────────────────────────────────────────────────
    frame_btns = tb.Frame(root)
    frame_btns.pack(pady=22)

    def al_hacer_click_generar():
        sel = combo.get()
        if not sel or sel not in producto_dict:
            messagebox.showwarning("Aviso", "Seleccioná un producto.")
            return

        try:
            cantidad = int(cantidad_entry.get())
            if cantidad <= 0:
                raise ValueError
        except Exception:
            messagebox.showwarning("Aviso", "Ingresá una cantidad válida (número entero mayor a 0).")
            return

        pid, desc = producto_dict[sel]

        def on_done(ultimo_serie: int):
            actualizar_label_cache(pid)

        generar_y_imprimir_qrs(pid, desc, cantidad, on_done_callback=on_done)

        cache_set_ui("ui_gen_producto", sel)
        cache_set_ui("ui_gen_cantidad", cantidad)

    def al_hacer_click_resetear():
        sel = combo.get()
        if not sel or sel not in producto_dict:
            messagebox.showwarning("Aviso", "Seleccioná un producto para resetear.")
            return

        pid, desc = producto_dict[sel]
        ultimo = cache_get_serie(pid)

        if ultimo == 0:
            messagebox.showinfo(
                "Sin cambios",
                f"El producto ya está en 0.\nLa próxima impresión arrancará desde N° de serie: 1."
            )
            return

        confirmar = messagebox.askyesno(
            "⚠️  ¿Estás seguro?",
            f"Vas a resetear el numero de serie del producto:\n\n"
            f"  {desc}\n\n"
            f"Último N° de serie registrado: {ultimo}\n\n"
            f"Si confirmás, la próxima impresión arrancará desde N° de serie: 1.\n\n"
            f"¿Querés continuar?",
            icon="warning"
        )

        if confirmar:
            cache_reset_serie(pid)
            actualizar_label_cache(pid)
            messagebox.showinfo(
                "Numero de serie reseteada",
                f"✅ El numero de serie de '{desc}' fue reseteada.\n"
                f"La próxima impresión arrancará desde N° de serie: 1."
            )

    tb.Button(
        frame_btns,
        text="  GENERAR QRs  ",
        bootstyle=SUCCESS,
        width=20,
        command=al_hacer_click_generar
    ).grid(row=0, column=0, padx=16)

    tb.Button(
        frame_btns,
        text=" INICIAR NUEVA LINEA DE PRODUCCION ",
        bootstyle=DANGER,
        width=40,
        command=al_hacer_click_resetear
    ).grid(row=0, column=1, padx=16)

    # ── Cierre limpio ─────────────────────────────────────────────────────────
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