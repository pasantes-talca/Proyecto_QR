import os, sys, json, logging
from datetime import date

import ttkbootstrap as tb
from ttkbootstrap.constants import *
from tkinter import messagebox, StringVar, filedialog

try:
    import psycopg2
    import psycopg2.extras
except ImportError:
    psycopg2 = None

try:
    from reportlab.lib.pagesizes import A4
    from reportlab.lib import colors
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from reportlab.lib.units import cm
    from reportlab.platypus import (
        SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, HRFlowable
    )
    from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_RIGHT
    HAS_REPORTLAB = True
except ImportError:
    HAS_REPORTLAB = False

# ── LOGGING ──────────────────────────────────────────────────────────────────
def _log_file_path():
    base = os.path.dirname(sys.executable if getattr(sys, "frozen", False) else os.path.abspath(__file__))
    return os.path.join(base, "defectos.log")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(_log_file_path(), encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)

# ── CONFIG ────────────────────────────────────────────────────────────────────
def get_app_dir():
    return os.path.dirname(sys.executable if getattr(sys, "frozen", False) else os.path.abspath(__file__))

APP_DIR     = get_app_dir()
CONFIG_FILE = os.path.join(APP_DIR, "config.json")

DEFAULT_PG = {
    "host":     os.getenv("TALCA_PG_HOST",     "localhost"),
    "port":     int(os.getenv("TALCA_PG_PORT", "5432")),
    "dbname":   os.getenv("TALCA_PG_DB",       "postgres"),
    "user":     os.getenv("TALCA_PG_USER",     "postgres"),
    "password": os.getenv("TALCA_PG_PASS",     ""),
    "schema":   "produccion",
    "table_products": "productos",
    "table_defectos": "defectos",
}

MOTIVOS = [
    "Sin gas",
    "Roto",
    "Etiqueta desteñida",
    "Envase deforme",
    "Problema Tapas",
    "Sabor",
]

def load_config():
    if not os.path.exists(CONFIG_FILE):
        return {}
    try:
        with open(CONFIG_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except Exception as exc:
        log.warning("No se pudo leer config.json: %s", exc)
        return {}

def get_pg_config():
    cfg  = DEFAULT_PG.copy()
    data = load_config()
    if isinstance(data.get("pg"), dict):
        for k, v in data["pg"].items():
            if v is not None and str(v).strip():
                cfg[k] = v
    try:
        cfg["port"] = int(cfg["port"])
    except Exception:
        cfg["port"] = 5432
    return cfg

def pg_connect():
    if psycopg2 is None:
        raise RuntimeError("Falta psycopg2. Instala con: pip install psycopg2-binary")
    cfg  = get_pg_config()
    conn = psycopg2.connect(
        host=cfg["host"], port=cfg["port"],
        dbname=cfg["dbname"], user=cfg["user"], password=cfg["password"],
    )
    conn.autocommit = False
    log.info("Conectado a %s:%s/%s", cfg["host"], cfg["port"], cfg["dbname"])
    return conn

# ── DB HELPERS ────────────────────────────────────────────────────────────────
def get_all_products(conn):
    cfg    = get_pg_config()
    schema = cfg["schema"]
    tprod  = cfg["table_products"]
    with conn.cursor() as cur:
        cur.execute(f"SELECT id, descripcion FROM {schema}.{tprod} ORDER BY id ASC;")
        return cur.fetchall()

def registrar_defecto(conn, id_producto, cantidad, lote, motivo):
    cfg    = get_pg_config()
    schema = cfg["schema"]
    tdef   = cfg["table_defectos"]
    hoy      = date.today()
    lote_val = int(lote) if lote and str(lote).strip().isdigit() else None
    with conn.cursor() as cur:
        cur.execute(
            f"""
            INSERT INTO {schema}.{tdef}
                (fecha, id_producto, cantidad, lote, motivo)
            VALUES
                (%s, %s, %s, %s, %s);
            """,
            (hoy, int(id_producto), int(cantidad), lote_val, motivo),
        )
    conn.commit()
    log.info("Defecto registrado: pid=%s cant=%s lote=%s motivo=%s",
             id_producto, cantidad, lote_val, motivo)

def get_defectos_reporte(conn, fecha_ini, fecha_fin):
    """
    Devuelve filas agrupadas por producto y motivo entre las fechas dadas.
    Retorna: [(descripcion, motivo, total_cantidad), ...]
    """
    cfg    = get_pg_config()
    schema = cfg["schema"]
    tdef   = cfg["table_defectos"]
    tprod  = cfg["table_products"]
    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT
                p.descripcion,
                d.motivo,
                SUM(d.cantidad) AS total
            FROM {schema}.{tdef} d
            INNER JOIN {schema}.{tprod} p ON p.id = d.id_producto
            WHERE d.fecha BETWEEN %s AND %s
            GROUP BY p.descripcion, d.motivo
            ORDER BY p.descripcion ASC, d.motivo ASC;
        """, (fecha_ini, fecha_fin))
        return cur.fetchall()

# ── PDF REPORT ────────────────────────────────────────────────────────────────
def generar_pdf(filepath, fecha_ini, fecha_fin, rows):
    """
    Genera un PDF con tabla agrupada por producto y motivo.
    rows: [(descripcion, motivo, cantidad), ...]
    """
    doc = SimpleDocTemplate(
        filepath,
        pagesize=A4,
        rightMargin=2*cm, leftMargin=2*cm,
        topMargin=2.5*cm, bottomMargin=2*cm,
    )

    styles = getSampleStyleSheet()
    style_title = ParagraphStyle(
        "titulo", parent=styles["Title"],
        fontSize=18, textColor=colors.HexColor("#1a5276"),
        spaceAfter=6, alignment=TA_CENTER,
    )
    style_sub = ParagraphStyle(
        "subtitulo", parent=styles["Normal"],
        fontSize=11, textColor=colors.HexColor("#555555"),
        spaceAfter=4, alignment=TA_CENTER,
    )
    style_footer = ParagraphStyle(
        "footer", parent=styles["Normal"],
        fontSize=9, textColor=colors.HexColor("#888888"),
        alignment=TA_CENTER,
    )

    story = []

    # Encabezado
    story.append(Paragraph("Reporte de Defectos", style_title))
    story.append(Paragraph(
        f"Período: {fecha_ini.strftime('%d/%m/%Y')}  —  {fecha_fin.strftime('%d/%m/%Y')}",
        style_sub,
    ))
    story.append(Spacer(1, 0.4*cm))
    story.append(HRFlowable(width="100%", thickness=1.5,
                             color=colors.HexColor("#1a5276")))
    story.append(Spacer(1, 0.5*cm))

    if not rows:
        story.append(Paragraph("No se encontraron defectos en el período seleccionado.",
                                styles["Normal"]))
        doc.build(story)
        return

    # ── Construir tabla ────────────────────────────────────────────────────
    # Agrupar subtotales por producto
    from collections import defaultdict
    subtotales = defaultdict(int)
    for desc, motivo, cant in rows:
        subtotales[desc] += int(cant)
    total_general = sum(subtotales.values())

    header = ["Producto", "Motivo", "Cantidad"]
    data   = [header]

    ultimo_prod = None
    for desc, motivo, cant in rows:
        if desc != ultimo_prod:
            ultimo_prod = desc
        data.append([desc, motivo, str(int(cant))])

        # Si es el último registro de este producto, agrego subtotal
        # Busco si el siguiente es distinto
        idx = rows.index((desc, motivo, cant))
        es_ultimo = (idx == len(rows) - 1) or (rows[idx + 1][0] != desc)
        if es_ultimo:
            data.append(["", f"Subtotal  {desc}", str(subtotales[desc])])

    # Fila total general
    data.append(["TOTAL GENERAL", "", str(total_general)])

    col_widths = [9*cm, 5.5*cm, 2.5*cm]
    tabla = Table(data, colWidths=col_widths, repeatRows=1)

    # Estilo de la tabla
    n    = len(data)
    ts   = TableStyle([
        # Encabezado
        ("BACKGROUND",   (0, 0), (-1, 0),  colors.HexColor("#1a5276")),
        ("TEXTCOLOR",    (0, 0), (-1, 0),  colors.white),
        ("FONTNAME",     (0, 0), (-1, 0),  "Helvetica-Bold"),
        ("FONTSIZE",     (0, 0), (-1, 0),  11),
        ("ALIGN",        (2, 0), (2, 0),   "CENTER"),
        ("BOTTOMPADDING",(0, 0), (-1, 0),  8),
        ("TOPPADDING",   (0, 0), (-1, 0),  8),

        # Cuerpo
        ("FONTNAME",     (0, 1), (-1, -2), "Helvetica"),
        ("FONTSIZE",     (0, 1), (-1, -2), 9),
        ("TOPPADDING",   (0, 1), (-1, -2), 5),
        ("BOTTOMPADDING",(0, 1), (-1, -2), 5),
        ("ALIGN",        (2, 1), (2, -1),  "CENTER"),
        ("VALIGN",       (0, 0), (-1, -1), "MIDDLE"),

        # Grilla
        ("GRID",         (0, 0), (-1, -2), 0.4, colors.HexColor("#cccccc")),
        ("LINEBELOW",    (0, 0), (-1, 0),  1.5, colors.HexColor("#1a5276")),

        # Fila total general
        ("BACKGROUND",   (0, -1), (-1, -1), colors.HexColor("#d5e8f7")),
        ("FONTNAME",     (0, -1), (-1, -1), "Helvetica-Bold"),
        ("FONTSIZE",     (0, -1), (-1, -1), 10),
        ("LINEABOVE",    (0, -1), (-1, -1), 1.5, colors.HexColor("#1a5276")),
    ])

    # Colorear filas alternas (solo filas de datos, no subtotales ni total)
    fila_data = 1
    for i, row in enumerate(data[1:], start=1):
        if row[0] and row[0] != "TOTAL GENERAL":
            if fila_data % 2 == 0:
                ts.add("BACKGROUND", (0, i), (-1, i), colors.HexColor("#eaf4fb"))
            fila_data += 1
        elif row[1].startswith("Subtotal"):
            # Filas de subtotal
            ts.add("BACKGROUND",  (0, i), (-1, i), colors.HexColor("#d6eaf8"))
            ts.add("FONTNAME",    (0, i), (-1, i), "Helvetica-Bold")
            ts.add("FONTSIZE",    (0, i), (-1, i), 9)
            ts.add("LINEABOVE",   (0, i), (-1, i), 0.8, colors.HexColor("#1a5276"))

    tabla.setStyle(ts)
    story.append(tabla)

    # Pie
    story.append(Spacer(1, 0.8*cm))
    story.append(HRFlowable(width="100%", thickness=0.5, color=colors.HexColor("#aaaaaa")))
    story.append(Spacer(1, 0.2*cm))
    story.append(Paragraph(
        f"Generado el {date.today().strftime('%d/%m/%Y')}  |  Total de defectos: {total_general}",
        style_footer,
    ))

    doc.build(story)
    log.info("PDF generado: %s", filepath)

# ── VENTANA DE REPORTE ────────────────────────────────────────────────────────
class ReporteWindow:
    def __init__(self, parent, conn):
        self.conn = conn
        self.win  = tb.Toplevel(parent)
        self.win.title("Generar Reporte PDF")
        self.win.geometry("420x300")
        self.win.resizable(False, False)
        self.win.grab_set()
        self._build()

    def _build(self):
        win = self.win
        tb.Label(win, text="Reporte de Defectos",
                 font=("Segoe UI", 16, "bold")).pack(pady=(20, 6))
        tb.Label(win, text="Seleccione el rango de fechas",
                 font=("Segoe UI", 10), foreground="gray").pack()

        frame = tb.Frame(win)
        frame.pack(pady=20, padx=30, fill="x")

        # Fecha inicio
        tb.Label(frame, text="Fecha inicio:", font=("Segoe UI", 11)).grid(
            row=0, column=0, sticky="e", padx=8, pady=10)
        self.ini_entry = tb.DateEntry(frame, dateformat="%Y-%m-%d",
                                      bootstyle=PRIMARY, width=14)
        self.ini_entry.grid(row=0, column=1, sticky="w", padx=8)

        # Fecha fin
        tb.Label(frame, text="Fecha fin:", font=("Segoe UI", 11)).grid(
            row=1, column=0, sticky="e", padx=8, pady=10)
        self.fin_entry = tb.DateEntry(frame, dateformat="%Y-%m-%d",
                                      bootstyle=PRIMARY, width=14)
        self.fin_entry.grid(row=1, column=1, sticky="w", padx=8)

        # Estado
        self.status_var = StringVar(value="")
        tb.Label(win, textvariable=self.status_var,
                 font=("Segoe UI", 9), foreground="#c0392b",
                 wraplength=380).pack(pady=4)

        # Botón
        tb.Button(win, text="  GENERAR PDF  ", bootstyle=PRIMARY,
                  width=22, command=self._generar).pack(pady=6)

    def _generar(self):
        if not HAS_REPORTLAB:
            messagebox.showerror(
                "Falta librería",
                "Instala reportlab:\n  pip install reportlab",
                parent=self.win,
            )
            return
        try:
            fecha_ini = self.ini_entry.entry.get().strip()
            fecha_fin = self.fin_entry.entry.get().strip()
            from datetime import datetime
            fi = datetime.strptime(fecha_ini, "%Y-%m-%d").date()
            ff = datetime.strptime(fecha_fin, "%Y-%m-%d").date()
            if fi > ff:
                raise ValueError("La fecha inicio no puede ser mayor a la fecha fin.")
        except ValueError as exc:
            self.status_var.set(f"⚠ {exc}")
            return

        filepath = filedialog.asksaveasfilename(
            parent=self.win,
            title="Guardar reporte PDF",
            defaultextension=".pdf",
            initialfile=f"defectos_{fi}_{ff}.pdf",
            filetypes=[("PDF files", "*.pdf")],
        )
        if not filepath:
            return

        try:
            rows = get_defectos_reporte(self.conn, fi, ff)
            generar_pdf(filepath, fi, ff, rows)
            messagebox.showinfo(
                "Listo",
                f"PDF generado correctamente.\n{filepath}",
                parent=self.win,
            )
            self.win.destroy()
        except Exception as exc:
            log.exception("Error al generar PDF")
            self.status_var.set(f"Error: {exc}")

# ── UI PRINCIPAL ──────────────────────────────────────────────────────────────
class DefectosApp:
    def __init__(self, conn):
        self.conn = conn
        self.root = tb.Window(themename="minty")
        self.root.title("Registro de Defectos")
        self.root.geometry("700x600")
        self.root.resizable(True, True)
        self._build_ui()
        self.root.mainloop()

    def _build_ui(self):
        root = self.root

        # Título
        tb.Label(root, text="Registro de Defectos",
                 font=("Segoe UI", 22, "bold")).pack(pady=(24, 12))

        # Formulario
        frame = tb.LabelFrame(root, text="Datos del defecto")
        frame.pack(pady=6, padx=50, fill="x", ipadx=10, ipady=10)
        frame.columnconfigure(1, weight=1)

        # Producto
        tb.Label(frame, text="Producto:", font=("Segoe UI", 11)).grid(
            row=0, column=0, sticky="e", padx=10, pady=10)
        prods = get_all_products(self.conn)
        self.prod_options = [f"{pid} — {desc}" for pid, desc in prods]
        self.prod_var = StringVar()
        self.prod_combo = tb.Combobox(frame, textvariable=self.prod_var,
                                      values=self.prod_options, state="readonly", width=55)
        self.prod_combo.grid(row=0, column=1, columnspan=2, sticky="w", padx=10, pady=10)
        if self.prod_options:
            self.prod_combo.current(0)

        # Cantidad
        tb.Label(frame, text="Cantidad de botellas:", font=("Segoe UI", 11)).grid(
            row=1, column=0, sticky="e", padx=10, pady=10)
        vcmd = (root.register(lambda v: v == "" or v.isdigit()), "%P")
        self.cant_var = StringVar(value="1")
        tb.Entry(frame, textvariable=self.cant_var, width=14,
                 validate="key", validatecommand=vcmd,
                 font=("Segoe UI", 11)).grid(row=1, column=1, sticky="w", padx=10, pady=10)

        # Lote (opcional)
        tb.Label(frame, text="Nº de lote (opcional):", font=("Segoe UI", 11)).grid(
            row=2, column=0, sticky="e", padx=10, pady=10)
        lote_vcmd = (root.register(lambda v: v == "" or v.isdigit()), "%P")
        self.lote_var = StringVar()
        tb.Entry(frame, textvariable=self.lote_var, width=20,
                 validate="key", validatecommand=lote_vcmd,
                 font=("Segoe UI", 11)).grid(row=2, column=1, sticky="w", padx=10, pady=10)
        tb.Label(frame, text="(dejar vacío si no aplica)",
                 font=("Segoe UI", 9, "italic"), foreground="gray").grid(
            row=2, column=2, sticky="w")

        # Motivo
        tb.Label(frame, text="Motivo del defecto:", font=("Segoe UI", 11)).grid(
            row=3, column=0, sticky="e", padx=10, pady=10)
        self.motivo_var = StringVar()
        self.motivo_combo = tb.Combobox(frame, textvariable=self.motivo_var,
                                        values=MOTIVOS, state="readonly", width=35,
                                        font=("Segoe UI", 11))
        self.motivo_combo.grid(row=3, column=1, sticky="w", padx=10, pady=10)
        self.motivo_combo.current(0)

        # Estado
        self.status_var = StringVar(value="Complete el formulario y presione REGISTRAR.")
        tb.Label(root, textvariable=self.status_var, font=("Segoe UI", 11),
                 wraplength=620, justify="left", foreground="#555").pack(pady=14, padx=50)

        # Botones
        btn_frame = tb.Frame(root)
        btn_frame.pack(pady=6)

        self.btn_reg = tb.Button(btn_frame, text="  REGISTRAR DEFECTO  ",
                                 bootstyle=DANGER, width=26, command=self._on_registrar)
        self.btn_reg.grid(row=0, column=0, padx=12)

        tb.Button(btn_frame, text="  GENERAR REPORTE PDF  ",
                  bootstyle=INFO, width=26,
                  command=self._on_reporte).grid(row=0, column=1, padx=12)

    def _on_registrar(self):
        try:
            pstr = self.prod_var.get()
            if not pstr:
                raise ValueError("Seleccione un producto.")
            id_producto = int(pstr.split(" — ")[0])
            cant_str = self.cant_var.get().strip()
            if not cant_str or not cant_str.isdigit():
                raise ValueError("La cantidad debe ser un número entero positivo.")
            cantidad = int(cant_str)
            if cantidad <= 0:
                raise ValueError("La cantidad debe ser mayor a cero.")
            lote   = self.lote_var.get().strip()
            motivo = self.motivo_var.get().strip()
            if not motivo:
                raise ValueError("Seleccione un motivo.")
        except ValueError as exc:
            self.status_var.set(f"⚠ {exc}")
            return

        desc     = pstr.split(" — ", 1)[1] if " — " in pstr else pstr
        lote_txt = lote if lote else "Sin especificar"
        if not messagebox.askyesno(
            "Confirmar registro",
            f"¿Registrar el siguiente defecto?\n\n"
            f"  Producto : {desc}\n"
            f"  Cantidad : {cantidad} botella(s)\n"
            f"  Lote     : {lote_txt}\n"
            f"  Motivo   : {motivo}",
            parent=self.root,
        ):
            return

        self.btn_reg.config(state="disabled", text="Guardando…")
        self.root.update_idletasks()
        try:
            registrar_defecto(self.conn, id_producto, cantidad, lote, motivo)
            self.status_var.set(
                f"✔ Defecto registrado.  Producto: {desc}  |  "
                f"Cantidad: {cantidad}  |  Lote: {lote_txt}  |  Motivo: {motivo}"
            )
            self.cant_var.set("1")
            self.lote_var.set("")
            self.motivo_combo.current(0)
        except Exception as exc:
            log.exception("Error al registrar defecto")
            self.status_var.set(f"✘ Error: {exc}")
        finally:
            self.btn_reg.config(state="normal", text="  REGISTRAR DEFECTO  ")

    def _on_reporte(self):
        ReporteWindow(self.root, self.conn)

# ── ENTRYPOINT ────────────────────────────────────────────────────────────────
def main():
    try:
        conn = pg_connect()
    except Exception as exc:
        messagebox.showerror("Error PostgreSQL", f"No se pudo conectar:\n{exc}")
        log.critical("Fallo de conexión: %s", exc)
        return
    DefectosApp(conn)

if __name__ == "__main__":
    main()