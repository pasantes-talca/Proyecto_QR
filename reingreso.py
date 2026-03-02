import os, sys, json, logging
from datetime import date, timedelta

import ttkbootstrap as tb
from ttkbootstrap.constants import *
from tkinter import messagebox, StringVar

try:
    import psycopg2
    import psycopg2.extras
except ImportError:
    psycopg2 = None

# ── LOGGING ─────────────────────────────────────────────────────────────────
def _log_file_path():
    base = os.path.dirname(sys.executable if getattr(sys, "frozen", False) else os.path.abspath(__file__))
    return os.path.join(base, "reingreso.log")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(), logging.FileHandler(_log_file_path(), encoding="utf-8")],
)
log = logging.getLogger(__name__)

# ── CONFIG ───────────────────────────────────────────────────────────────────
def get_app_dir():
    return os.path.dirname(sys.executable if getattr(sys, "frozen", False) else os.path.abspath(__file__))

APP_DIR     = get_app_dir()
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
    "table_bajas": "bajas",
    "table_sheet": "sheet",
    "dias_vencimiento": int(os.getenv("TALCA_DIAS_VENC", "180")),
}

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
    cfg = DEFAULT_PG.copy()
    data = load_config()
    if isinstance(data.get("pg"), dict):
        for k, v in data["pg"].items():
            if v is not None and str(v).strip():
                cfg[k] = v
    try:   cfg["port"] = int(cfg["port"])
    except: cfg["port"] = 5432
    try:   cfg["dias_vencimiento"] = int(cfg.get("dias_vencimiento", 180))
    except: cfg["dias_vencimiento"] = 180
    return cfg

def pg_connect():
    if psycopg2 is None:
        raise RuntimeError("Falta psycopg2. Instala con: pip install psycopg2-binary")
    cfg = get_pg_config()
    conn = psycopg2.connect(
        host=cfg["host"], port=cfg["port"],
        dbname=cfg["dbname"], user=cfg["user"], password=cfg["password"],
    )
    conn.autocommit = False  # IMPORTANTE: transacciones manuales
    enc = cfg.get("client_encoding", "").strip()
    if enc:
        conn.set_client_encoding(enc)
    log.info("Conectado a %s:%s/%s", cfg["host"], cfg["port"], cfg["dbname"])
    return conn

# ── DB HELPERS ────────────────────────────────────────────────────────────────
def _t(cfg):
    return cfg["schema"], cfg["table_products"], cfg["table_stock"], cfg["table_bajas"]

def get_product_desc(conn, id_producto):
    cfg = get_pg_config()
    schema, tprod, *_ = _t(cfg)
    with conn.cursor() as cur:
        cur.execute(f"SELECT descripcion FROM {schema}.{tprod} WHERE id=%s;", (id_producto,))
        row = cur.fetchone()
    return str(row[0]).strip() if row else "Sin descripcion"

def get_all_products(conn):
    """Solo productos que tengan bajas con motivo='Venta'."""
    cfg = get_pg_config()
    schema, tprod, _, tbajas = _t(cfg)
    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT DISTINCT p.id, p.descripcion
            FROM {schema}.{tprod} p
            INNER JOIN {schema}.{tbajas} b ON b.id_producto = p.id
            WHERE b.motivo = 'Venta'
            ORDER BY p.id ASC;
        """)
        return cur.fetchall()

def get_lotes_con_bajas(conn, id_producto):
    """Solo lotes con motivo='Venta'."""
    cfg = get_pg_config()
    schema, _, _, tbajas = _t(cfg)
    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT DISTINCT stock_lote FROM {schema}.{tbajas}
            WHERE id_producto=%s
              AND stock_lote IS NOT NULL
              AND motivo = 'Venta'
            ORDER BY stock_lote ASC;
        """, (id_producto,))
        return [r[0] for r in cur.fetchall()]

def get_cantidad_baja_por_lote_tipo(conn, id_producto, lote, tipo_unidad):
    """Suma cantidad disponible para reingresar (solo motivo='Venta')."""
    cfg = get_pg_config()
    schema, _, _, tbajas = _t(cfg)
    tipo_unidad = tipo_unidad.upper()
    with conn.cursor() as cur:
        if tipo_unidad == "PALLET":
            cur.execute(f"""
                SELECT COALESCE(SUM(cantidad), 0) FROM {schema}.{tbajas}
                WHERE id_producto=%s AND stock_lote=%s
                  AND motivo = 'Venta'
                  AND (tipo_unidad='PALLET' OR tipo_unidad IS NULL);
            """, (id_producto, lote))
        else:
            cur.execute(f"""
                SELECT COALESCE(SUM(cantidad), 0) FROM {schema}.{tbajas}
                WHERE id_producto=%s AND stock_lote=%s
                  AND motivo = 'Venta'
                  AND tipo_unidad='PACKS';
            """, (id_producto, lote))
        result = cur.fetchone()
    return int(result[0]) if result and result[0] is not None else 0

def _neto_stock(conn, pid, schema, tstock):
    try:
        with conn.cursor() as cur:
            cur.execute(f"""
                SELECT
                    COUNT(*) FILTER (WHERE tipo_unidad='PALLET') AS pallets,
                    COUNT(*) FILTER (WHERE tipo_unidad='PACKS')  AS packs
                FROM {schema}.{tstock} WHERE id_producto=%s;
            """, (pid,))
            row = cur.fetchone()
        return int(row[0]), int(row[1]), ""
    except Exception as exc:
        log.warning("No se pudo calcular neto: %s", exc)
        return 0, 0, "No se pudo calcular el neto actual."

def _descontar_bajas(cur, schema, tbajas, id_producto, lote, tipo_unidad, cantidad_a_descontar):
    """
    Descuenta de la tabla bajas (motivo='Venta') de forma FIFO:
    - Si la fila tiene <= cantidad a descontar: la ELIMINA.
    - Si tiene mas: REDUCE su cantidad.
    Usa FOR UPDATE para evitar race conditions.
    """
    tipo_unidad = tipo_unidad.upper()
    if tipo_unidad == "PALLET":
        tipo_filter = "(tipo_unidad='PALLET' OR tipo_unidad IS NULL)"
    else:
        tipo_filter = "tipo_unidad='PACKS'"

    cur.execute(f"""
        SELECT id, cantidad FROM {schema}.{tbajas}
        WHERE id_producto=%s AND stock_lote=%s
          AND motivo='Venta'
          AND {tipo_filter}
        ORDER BY id ASC
        FOR UPDATE;
    """, (id_producto, lote))

    filas    = cur.fetchall()
    restante = cantidad_a_descontar

    for fila_id, fila_cant in filas:
        if restante <= 0:
            break
        if fila_cant <= restante:
            # Eliminar fila completa
            cur.execute(f"DELETE FROM {schema}.{tbajas} WHERE id=%s;", (fila_id,))
            log.info("Baja eliminada: id=%s cantidad=%s", fila_id, fila_cant)
            restante -= fila_cant
        else:
            # Reducir cantidad parcialmente
            nueva_cant = fila_cant - restante
            cur.execute(f"UPDATE {schema}.{tbajas} SET cantidad=%s WHERE id=%s;", (nueva_cant, fila_id))
            log.info("Baja reducida: id=%s de %s a %s", fila_id, fila_cant, nueva_cant)
            restante = 0

    if restante > 0:
        raise ValueError(f"No hay suficiente cantidad en bajas. Faltaron {restante} unidades.")

def reingresar_al_stock(conn, id_producto, lote, tipo_unidad, cantidad):
    """
    Transaccion atomica:
    1. Inserta 'cantidad' filas en stock.
    2. Descuenta/elimina las filas correspondientes en bajas (motivo='Venta').
    Si algo falla hace ROLLBACK completo: ni stock ni bajas quedan alterados.
    """
    pid         = int(id_producto)
    lote        = str(lote).strip()
    tipo_unidad = tipo_unidad.upper()

    if tipo_unidad not in ("PALLET", "PACKS"):
        raise ValueError(f"tipo_unidad invalido: {tipo_unidad!r}")
    if cantidad <= 0:
        raise ValueError("La cantidad debe ser mayor a cero.")

    packs_valor = 0 if tipo_unidad == "PALLET" else 1
    cfg         = get_pg_config()
    schema, _, tstock, tbajas = _t(cfg)
    fecha_hoy   = date.today()
    fecha_venc  = fecha_hoy + timedelta(days=cfg.get("dias_vencimiento", 180))

    try:
        with conn.cursor() as cur:
            # 1. Insertar en stock
            cur.execute(f"SELECT COALESCE(MAX(nro_serie), 0)+1 FROM {schema}.{tstock}")
            next_serie = cur.fetchone()[0]

            rows = [
                (pid, next_serie + i, lote, fecha_hoy, fecha_venc, tipo_unidad, packs_valor)
                for i in range(cantidad)
            ]
            psycopg2.extras.execute_values(
                cur,
                f"""
                INSERT INTO {schema}.{tstock}
                    (id_producto, nro_serie, lote, creacion, vencimiento, fecha_hora, tipo_unidad, packs)
                VALUES %s
                """,
                rows,
                template="(%s,%s,%s,%s,%s,NOW(),%s,%s)",
            )
            log.info("Stock insertado: pid=%s lote=%s tipo=%s qty=%s", pid, lote, tipo_unidad, cantidad)

            # 2. Descontar/eliminar de bajas
            _descontar_bajas(cur, schema, tbajas, pid, lote, tipo_unidad, cantidad)

        conn.commit()
        log.info("COMMIT exitoso: pid=%s lote=%s tipo=%s qty=%s", pid, lote, tipo_unidad, cantidad)

    except Exception as exc:
        conn.rollback()
        log.error("ROLLBACK por error: %s", exc)
        raise

    desc = get_product_desc(conn, pid)
    net_p, net_pk, warn = _neto_stock(conn, pid, schema, tstock)
    return pid, desc, lote, tipo_unidad, cantidad, net_p, net_pk, warn

# ── UI ────────────────────────────────────────────────────────────────────────
class ReingresoApp:
    def __init__(self, conn):
        self.conn = conn
        self.root = tb.Window(themename="minty")
        self.root.title("Reingreso desde Bajas - Manual")
        self.root.geometry("980x640")
        self.root.resizable(True, True)
        self._build_ui()
        self._precargar_primer_producto()
        self.root.mainloop()

    def _build_ui(self):
        root = self.root
        tb.Label(root, text="Reingreso desde Bajas al Stock",
                 font=("Segoe UI", 22, "bold")).pack(pady=(20, 10))

        frame = tb.LabelFrame(root, text="Datos del reingreso")
        frame.pack(pady=10, padx=50, fill="x")

        # Producto
        tb.Label(frame, text="Producto:").grid(row=0, column=0, sticky="e", padx=10, pady=10)
        prods = get_all_products(self.conn)
        self.prod_options = [f"{pid} - {desc}" for pid, desc in prods]
        self.prod_var = StringVar()
        self.prod_combo = tb.Combobox(frame, textvariable=self.prod_var,
                                      values=self.prod_options, width=65)
        self.prod_combo.grid(row=0, column=1, columnspan=3, sticky="w", padx=10, pady=10)
        self.prod_combo.bind("<<ComboboxSelected>>", self._on_producto)

        # Lote
        tb.Label(frame, text="Lote con bajas:").grid(row=1, column=0, sticky="e", padx=10, pady=10)
        self.lote_var = StringVar()
        self.lote_combo = tb.Combobox(frame, textvariable=self.lote_var, width=40, state="readonly")
        self.lote_combo.grid(row=1, column=1, sticky="w", padx=10, pady=10)
        self.lote_combo.bind("<<ComboboxSelected>>", self._on_lote_tipo)

        self.max_var = StringVar(value="Max disponible: -")
        tb.Label(frame, textvariable=self.max_var,
                 font=("Segoe UI", 10, "italic")).grid(row=1, column=2, sticky="w", padx=15)

        # Tipo unidad
        tb.Label(frame, text="Tipo a reingresar:").grid(row=2, column=0, sticky="e", padx=10, pady=10)
        self.type_var = StringVar(value="PALLET")
        tb.Radiobutton(frame, text="Pallets", variable=self.type_var, value="PALLET").grid(row=2, column=1, sticky="w", padx=10)
        tb.Radiobutton(frame, text="Packs",   variable=self.type_var, value="PACKS").grid(row=2, column=2, sticky="w")
        self.type_var.trace_add("write", self._on_lote_tipo)

        # Cantidad
        tb.Label(frame, text="Cantidad a reingresar:").grid(row=3, column=0, sticky="e", padx=10, pady=10)
        self.cant_var = StringVar(value="1")
        vcmd = (root.register(lambda v: v == "" or v.isdigit()), "%P")
        tb.Entry(frame, textvariable=self.cant_var, width=12,
                 validate="key", validatecommand=vcmd).grid(row=3, column=1, sticky="w", padx=10)

        # Estado
        self.status_var = StringVar(value="Seleccione producto y lote para ver disponibilidad")
        tb.Label(root, textvariable=self.status_var, font=("Segoe UI", 11),
                 wraplength=880, justify="left").pack(pady=15, padx=50)

        # Boton principal
        self.btn = tb.Button(root, text="REALIZAR REINGRESO", bootstyle=SUCCESS,
                             width=30, command=self._on_reingreso)
        self.btn.pack(pady=20)

    def _on_producto(self, _e=None):
        val = self.prod_var.get()
        if not val:
            self.lote_combo["values"] = []; self.lote_var.set("")
            self.max_var.set("Max disponible: -")
            self.status_var.set("Seleccione un producto"); return
        try:
            pid   = int(val.split(" - ")[0])
            lotes = get_lotes_con_bajas(self.conn, pid)
            if not lotes:
                self.status_var.set("No hay bajas con motivo Venta para este producto")
                self.lote_combo["values"] = []; self.lote_var.set("")
                self.max_var.set("Max disponible: 0"); return
            self.lote_combo["values"] = lotes
            self.lote_var.set(lotes[0])
            self._actualizar_max()
        except Exception as exc:
            log.exception("Error al cargar lotes")
            self.status_var.set(f"Error al cargar lotes: {exc}")

    def _on_lote_tipo(self, *_):
        self._actualizar_max()

    def _actualizar_max(self):
        if not self.lote_var.get() or not self.prod_var.get():
            self.max_var.set("Max disponible: -"); return
        try:
            pid      = int(self.prod_var.get().split(" - ")[0])
            max_cant = get_cantidad_baja_por_lote_tipo(
                self.conn, pid, self.lote_var.get(), self.type_var.get())
            self.max_var.set(f"Max disponible: {max_cant} {self.type_var.get()}")
        except Exception as exc:
            log.exception("Error al calcular maximo")
            self.max_var.set("Max disponible: -")
            self.status_var.set(f"Error al calcular maximo: {exc}")

    def _on_reingreso(self):
        try:
            pstr = self.prod_var.get()
            if not pstr: raise ValueError("Seleccione un producto.")
            pid  = int(pstr.split(" - ")[0])
            lote = self.lote_var.get()
            if not lote: raise ValueError("Seleccione un lote.")
            tipo    = self.type_var.get()
            qty_str = self.cant_var.get().strip()
            if not qty_str or not qty_str.isdigit():
                raise ValueError("La cantidad debe ser un numero entero positivo.")
            qty = int(qty_str)
            if qty <= 0: raise ValueError("La cantidad debe ser mayor a cero.")
            max_disp = get_cantidad_baja_por_lote_tipo(self.conn, pid, lote, tipo)
            if qty > max_disp:
                raise ValueError(f"No puede reingresar mas de {max_disp} {tipo} en este lote.")
        except ValueError as exc:
            self.status_var.set(f"Error: {exc}"); return

        desc_prev = pstr.split(" - ", 1)[1] if " - " in pstr else pstr
        if not messagebox.askyesno(
            "Confirmar reingreso",
            f"Confirmar el reingreso de {qty} {tipo}\nLote: {lote}\nProducto: {desc_prev}?",
            parent=self.root,
        ):
            return

        self.btn.config(state="disabled", text="Procesando...")
        self.root.update_idletasks()
        try:
            pid_r, desc, lote_r, tipo_u, cant, net_p, net_pk, warn = \
                reingresar_al_stock(self.conn, pid, lote, tipo, qty)
            msg = (f"Reingreso registrado (motivo: Venta)\n"
                   f"Producto: {pid_r} - {desc}\n"
                   f"Lote: {lote_r}  |  Tipo: {tipo_u}  |  Cantidad: {cant}\n"
                   f"Neto actual: Pallets={net_p}  Packs={net_pk}")
            if warn: msg += f"\n{warn}"
            self.status_var.set(msg)
            self._actualizar_max()
        except Exception as exc:
            log.exception("Error al realizar reingreso")
            self.status_var.set(f"Error: {exc}")
        finally:
            self.btn.config(state="normal", text="REALIZAR REINGRESO")

    def _precargar_primer_producto(self):
        if self.prod_options:
            self.prod_var.set(self.prod_options[0])
            self._on_producto()

# ── ENTRYPOINT ────────────────────────────────────────────────────────────────
def main():
    try:
        conn = pg_connect()
    except Exception as exc:
        messagebox.showerror("Error PostgreSQL", f"No se pudo conectar:\n{exc}")
        log.critical("Fallo de conexion: %s", exc)
        return
    ReingresoApp(conn)

if __name__ == "__main__":
    main()