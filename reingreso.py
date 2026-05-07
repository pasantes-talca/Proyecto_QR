import os, sys, json, logging, urllib.request, urllib.error
import threading
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

# ── MOTIVOS DISPONIBLES (deben coincidir con salida.py) ──────────────────────
MOTIVOS = ("Venta", "Calidad", "Desarme", "Observacion")

# ── CONFIG ───────────────────────────────────────────────────────────────────
APPS_SCRIPT_URL = "https://script.google.com/macros/s/AKfycbwWisnYakHygj12AhHTggRS2ugLDV5jXmxPYiQsdvVUwCkCEjLZySXYpMa1hQhpSUSJ/exec"

def get_app_dir():
    return os.path.dirname(sys.executable if getattr(sys, "frozen", False) else os.path.abspath(__file__))

APP_DIR     = get_app_dir()
CONFIG_FILE = os.path.join(APP_DIR, "config.json")

DEFAULT_PG = {
    "host":"10.242.4.13",
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
    conn.autocommit = False
    enc = cfg.get("client_encoding", "").strip()
    if enc:
        conn.set_client_encoding(enc)
    log.info("Conectado a %s:%s/%s", cfg["host"], cfg["port"], cfg["dbname"])
    return conn

# ── DB HELPERS ────────────────────────────────────────────────────────────────
def _t(cfg):
    return cfg["schema"], cfg["table_products"], cfg["table_stock"], cfg["table_bajas"]


def get_all_products(conn, motivo: str):
    """Productos que tienen bajas con el motivo indicado."""
    cfg = get_pg_config()
    schema, tprod, _, tbajas = _t(cfg)
    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT DISTINCT p.id, p.descripcion
            FROM {schema}.{tprod} p
            INNER JOIN {schema}.{tbajas} b ON b.id_producto = p.id
            WHERE b.motivo = %s
            ORDER BY p.id ASC;
        """, (motivo,))
        return cur.fetchall()


def get_lotes_con_bajas(conn, id_producto: int, motivo: str):
    """Lotes que tienen bajas con el motivo indicado para ese producto."""
    cfg = get_pg_config()
    schema, _, _, tbajas = _t(cfg)
    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT DISTINCT stock_lote FROM {schema}.{tbajas}
            WHERE id_producto=%s AND motivo=%s AND stock_lote IS NOT NULL
            ORDER BY stock_lote ASC;
        """, (id_producto, motivo))
        return [r[0] for r in cur.fetchall()]


def get_cantidad_baja_por_lote_tipo(conn, id_producto: int, lote: str,
                                    tipo_unidad: str, motivo: str):
    """Cantidad disponible para reingreso según motivo, lote y tipo."""
    cfg = get_pg_config()
    schema, _, _, tbajas = _t(cfg)
    tipo_unidad = tipo_unidad.upper()
    with conn.cursor() as cur:
        if tipo_unidad == "PALLET":
            cur.execute(f"""
                SELECT COALESCE(SUM(cantidad), 0) FROM {schema}.{tbajas}
                WHERE id_producto=%s AND stock_lote=%s
                  AND motivo=%s
                  AND (tipo_unidad='PALLET' OR tipo_unidad IS NULL);
            """, (id_producto, lote, motivo))
        else:
            cur.execute(f"""
                SELECT COALESCE(SUM(cantidad), 0) FROM {schema}.{tbajas}
                WHERE id_producto=%s AND stock_lote=%s
                  AND motivo=%s AND tipo_unidad='PACKS';
            """, (id_producto, lote, motivo))
        result = cur.fetchone()
    return int(result[0]) if result and result[0] is not None else 0


def get_product_net_stock(conn, id_producto: int):
    cfg = get_pg_config()
    schema, tprod, tstock, _ = _t(cfg)
    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT 
                p.descripcion,
                COUNT(CASE WHEN s.tipo_unidad='PALLET' THEN 1 END) AS pallets,
                COALESCE(SUM(CASE WHEN s.tipo_unidad='PACKS' THEN s.packs ELSE 0 END), 0) AS packs
            FROM {schema}.{tprod} p
            LEFT JOIN {schema}.{tstock} s ON s.id_producto = p.id
            WHERE p.id = %s
            GROUP BY p.descripcion;
        """, (id_producto,))
        row = cur.fetchone()
        if row and row[0]:
            return str(row[0]).strip(), int(row[1] or 0), int(row[2] or 0)
        return "Sin descripcion", 0, 0


def _descontar_bajas(cur, schema, tbajas, id_producto, lote,
                     tipo_unidad, cantidad_a_descontar, motivo):
    """Descuenta bajas del motivo indicado (FIFO por id)."""
    tipo_unidad = tipo_unidad.upper()
    tipo_filter = "(tipo_unidad='PALLET' OR tipo_unidad IS NULL)" if tipo_unidad == "PALLET" else "tipo_unidad='PACKS'"

    cur.execute(f"""
        SELECT id, cantidad FROM {schema}.{tbajas}
        WHERE id_producto=%s AND stock_lote=%s
          AND motivo=%s AND {tipo_filter}
        ORDER BY id ASC FOR UPDATE;
    """, (id_producto, lote, motivo))

    filas = cur.fetchall()
    restante = cantidad_a_descontar

    for fila_id, fila_cant in filas:
        if restante <= 0:
            break
        if fila_cant <= restante:
            cur.execute(f"DELETE FROM {schema}.{tbajas} WHERE id=%s;", (fila_id,))
            log.info("Baja eliminada: id=%s cantidad=%s motivo=%s", fila_id, fila_cant, motivo)
            restante -= fila_cant
        else:
            nueva_cant = fila_cant - restante
            cur.execute(f"UPDATE {schema}.{tbajas} SET cantidad=%s WHERE id=%s;", (nueva_cant, fila_id))
            log.info("Baja reducida: id=%s de %s a %s motivo=%s", fila_id, fila_cant, nueva_cant, motivo)
            restante = 0

    if restante > 0:
        raise ValueError(f"No hay suficiente cantidad en bajas ({motivo}). Faltaron {restante} unidades.")


# ── GOOGLE SHEETS SYNC (se usa en background) ─────────────────────────────────
def sync_sheet_after_reingreso(conn):
    url = "https://script.google.com/macros/s/AKfycbwWisnYakHygj12AhHTggRS2ugLDV5jXmxPYiQsdvVUwCkCEjLZySXYpMa1hQhpSUSJ/exec"
    if not url or "TU_ID_AQUI" in url:
        log.warning("APPS_SCRIPT_URL no configurada.")
        return False, "URL de Apps Script no configurada."

    try:
        cfg = get_pg_config()
        schema, tprod, tstock, _ = _t(cfg)
        with conn.cursor() as cur:
            cur.execute(f"""
                SELECT
                    p.descripcion,
                    COUNT(s.id) FILTER (WHERE s.tipo_unidad='PALLET') AS stock_pallets,
                    COALESCE(SUM(s.packs), 0) AS stock_packs
                FROM {schema}.{tprod} p
                LEFT JOIN {schema}.{tstock} s ON s.id_producto = p.id
                GROUP BY p.id, p.descripcion
                ORDER BY p.descripcion ASC;
            """)
            rows = cur.fetchall()

        payload_rows = [
            {"descripcion": str(desc).strip(), "stock_pallets": int(p or 0), "stock_packs": int(pk or 0)}
            for desc, p, pk in rows
        ]

        payload = {
            "action":         "bulk_snapshot_pp",
            "snapshot_id":    f"reingreso_{date.today().isoformat()}",
            "is_first_block": True,
            "is_last_block":  True,
            "rows":           payload_rows,
        }

        body = json.dumps(payload).encode("utf-8")
        req  = urllib.request.Request(url, data=body, headers={"Content-Type": "application/json"}, method="POST")

        opener = urllib.request.build_opener(urllib.request.HTTPRedirectHandler())
        with opener.open(req, timeout=15) as resp:
            data = json.loads(resp.read().decode("utf-8"))

        if data.get("ok"):
            log.info("Google Sheets sincronizado: %s filas", data.get("wrote", "?"))
            return True, f"Sheet actualizado ({data.get('wrote', '?')} productos)."
        else:
            return False, f"Apps Script error: {data.get('error', str(data))}"

    except Exception as exc:
        log.warning("Error sync sheet: %s", exc)
        return False, f"Error sync: {exc}"


# ── CORE REINGRESO ─────────────────────────────────────────────────────────────
def reingresar_al_stock(conn, id_producto: int, lote: str,
                        tipo_unidad: str, cantidad: int, motivo: str):
    pid = int(id_producto)
    lote = str(lote).strip()
    tipo_unidad = tipo_unidad.upper()

    if tipo_unidad not in ("PALLET", "PACKS"):
        raise ValueError(f"tipo_unidad inválido: {tipo_unidad!r}")
    if cantidad <= 0:
        raise ValueError("La cantidad debe ser mayor a cero.")

    packs_valor = 0 if tipo_unidad == "PALLET" else 1
    cfg = get_pg_config()
    schema, _, tstock, tbajas = _t(cfg)
    fecha_hoy = date.today()
    fecha_venc = fecha_hoy + timedelta(days=cfg.get("dias_vencimiento", 180))

    try:
        with conn.cursor() as cur:
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

            _descontar_bajas(cur, schema, tbajas, pid, lote, tipo_unidad, cantidad, motivo)

        conn.commit()
        log.info("COMMIT exitoso: pid=%s lote=%s tipo=%s qty=%s motivo=%s",
                 pid, lote, tipo_unidad, cantidad, motivo)

    except Exception as exc:
        conn.rollback()
        log.error("ROLLBACK: %s", exc)
        raise

    desc, net_p, net_pk = get_product_net_stock(conn, pid)
    return pid, desc, lote, tipo_unidad, cantidad, net_p, net_pk


# ── UI ────────────────────────────────────────────────────────────────────────
class ReingresoApp:
    def __init__(self, conn):
        self.conn = conn
        self.root = tb.Window(themename="minty")
        self.root.title("Reingreso desde Bajas al Stock")
        self.root.geometry("1000x700")
        self.root.resizable(True, True)
        self._build_ui()
        self._on_motivo_changed()   # carga inicial según motivo por defecto
        self.root.mainloop()

    def _build_ui(self):
        root = self.root
        tb.Label(root, text="Reingreso desde Bajas al Stock",
                 font=("Segoe UI", 22, "bold")).pack(pady=(20, 6))

        # ── Selector de Motivo ───────────────────────────────────────────────
        motivo_frame = tb.Labelframe(root, text="Filtrar por motivo de baja")
        motivo_frame.pack(pady=(0, 8), padx=50, fill="x")

        self.motivo_var = StringVar(value="Venta")
        for m in MOTIVOS:
            tb.Radiobutton(
                motivo_frame, text=m, variable=self.motivo_var, value=m,
                command=self._on_motivo_changed
            ).pack(side="left", padx=18, pady=8)

        # ── Datos del reingreso ──────────────────────────────────────────────
        frame = tb.Labelframe(root, text="Datos del reingreso")
        frame.pack(pady=6, padx=50, fill="x")

        tb.Label(frame, text="Producto:").grid(row=0, column=0, sticky="e", padx=10, pady=10)
        self.prod_var = StringVar()
        self.prod_combo = tb.Combobox(frame, textvariable=self.prod_var,
                                      values=[], width=65)
        self.prod_combo.grid(row=0, column=1, columnspan=3, sticky="w", padx=10, pady=10)
        self.prod_combo.bind("<<ComboboxSelected>>", self._on_producto)

        tb.Label(frame, text="Lote con bajas:").grid(row=1, column=0, sticky="e", padx=10, pady=10)
        self.lote_var = StringVar()
        self.lote_combo = tb.Combobox(frame, textvariable=self.lote_var, width=40, state="readonly")
        self.lote_combo.grid(row=1, column=1, sticky="w", padx=10, pady=10)
        self.lote_combo.bind("<<ComboboxSelected>>", self._on_lote_tipo)

        self.max_var = StringVar(value="Max disponible: -")
        tb.Label(frame, textvariable=self.max_var,
                 font=("Segoe UI", 10, "italic")).grid(row=1, column=2, sticky="w", padx=15)

        tb.Label(frame, text="Tipo a reingresar:").grid(row=2, column=0, sticky="e", padx=10, pady=10)
        self.type_var = StringVar(value="PALLET")
        tb.Radiobutton(frame, text="Pallets", variable=self.type_var,
                       value="PALLET").grid(row=2, column=1, sticky="w", padx=10)
        tb.Radiobutton(frame, text="Packs", variable=self.type_var,
                       value="PACKS").grid(row=2, column=2, sticky="w")
        self.type_var.trace_add("write", self._on_lote_tipo)

        tb.Label(frame, text="Cantidad a reingresar:").grid(row=3, column=0, sticky="e", padx=10, pady=10)
        self.cant_var = StringVar(value="1")
        vcmd = (root.register(lambda v: v == "" or v.isdigit()), "%P")
        tb.Entry(frame, textvariable=self.cant_var, width=12,
                 validate="key", validatecommand=vcmd).grid(row=3, column=1, sticky="w", padx=10)

        # ── Estado y botón ───────────────────────────────────────────────────
        self.status_var = StringVar(value="Seleccione motivo, producto y lote.")
        tb.Label(root, textvariable=self.status_var, font=("Segoe UI", 11),
                 wraplength=900, justify="left").pack(pady=12, padx=50)

        self.btn = tb.Button(root, text="REALIZAR REINGRESO", bootstyle=SUCCESS,
                             width=30, command=self._on_reingreso)
        self.btn.pack(pady=16)

    # ── Callbacks ──────────────────────────────────────────────────────────────
    def _on_motivo_changed(self):
        """Al cambiar de motivo recarga la lista de productos disponibles."""
        motivo = self.motivo_var.get()
        prods  = get_all_products(self.conn, motivo)
        options = [f"{pid} - {desc}" for pid, desc in prods]
        self.prod_combo["values"] = options

        if options:
            self.prod_var.set(options[0])
            self._on_producto()
        else:
            self.prod_var.set("")
            self.lote_combo["values"] = []
            self.lote_var.set("")
            self.max_var.set("Max disponible: -")
            self.status_var.set(
                f"ℹ️ No hay productos con bajas por motivo '{motivo}'.")

    def _on_producto(self, _e=None):
        val = self.prod_var.get()
        if not val:
            self.lote_combo["values"] = []
            self.lote_var.set("")
            self.max_var.set("Max disponible: -")
            return
        try:
            pid    = int(val.split(" - ")[0])
            motivo = self.motivo_var.get()
            lotes  = get_lotes_con_bajas(self.conn, pid, motivo)
            self.lote_combo["values"] = lotes
            if lotes:
                self.lote_var.set(lotes[0])
            self._actualizar_max()
        except Exception as exc:
            log.exception("Error al cargar lotes")
            self.status_var.set(f"Error al cargar lotes: {exc}")

    def _on_lote_tipo(self, *_):
        self._actualizar_max()

    def _actualizar_max(self):
        if not self.lote_var.get() or not self.prod_var.get():
            self.max_var.set("Max disponible: -")
            return
        try:
            pid    = int(self.prod_var.get().split(" - ")[0])
            motivo = self.motivo_var.get()
            max_cant = get_cantidad_baja_por_lote_tipo(
                self.conn, pid, self.lote_var.get(), self.type_var.get(), motivo)
            self.max_var.set(f"Max disponible: {max_cant} {self.type_var.get()}")
        except Exception as exc:
            log.exception("Error al calcular máximo")
            self.max_var.set("Max disponible: -")

    def _refresh_sheet_background(self):
        def _do_sync():
            sheet_ok, sheet_msg = sync_sheet_after_reingreso(self.conn)
            if not sheet_ok:
                def _update_ui():
                    current = self.status_var.get()
                    self.status_var.set(f"{current}\n⚠️ {sheet_msg}")
                self.root.after(0, _update_ui)
        threading.Thread(target=_do_sync, daemon=True).start()

    def _on_reingreso(self):
        try:
            pstr = self.prod_var.get()
            if not pstr: raise ValueError("Seleccione un producto.")
            pid = int(pstr.split(" - ")[0])
            lote = self.lote_var.get()
            if not lote: raise ValueError("Seleccione un lote.")
            tipo   = self.type_var.get()
            motivo = self.motivo_var.get()
            qty_str = self.cant_var.get().strip()
            if not qty_str.isdigit(): raise ValueError("Cantidad debe ser número entero.")
            qty = int(qty_str)
            if qty <= 0: raise ValueError("Cantidad debe ser mayor a cero.")

            max_disp = get_cantidad_baja_por_lote_tipo(
                self.conn, pid, lote, tipo, motivo)
            if qty > max_disp:
                raise ValueError(
                    f"No puede reingresar más de {max_disp} {tipo} "
                    f"(motivo: {motivo}).")
        except ValueError as exc:
            self.status_var.set(f"Error: {exc}")
            return

        desc_prev = pstr.split(" - ", 1)[1] if " - " in pstr else pstr
        if not messagebox.askyesno(
                "Confirmar reingreso",
                f"Confirmar reingreso de {qty} {tipo}\n"
                f"Lote: {lote}\nProducto: {desc_prev}\nMotivo de baja: {motivo}?",
                parent=self.root):
            return

        self.btn.config(state="disabled", text="Procesando...")
        self.root.update_idletasks()

        try:
            pid_r, desc, lote_r, tipo_u, cant, net_p, net_pk = reingresar_al_stock(
                self.conn, pid, lote, tipo, qty, motivo)

            msg = (
                f"✅ Reingreso registrado\n"
                f"Producto: {pid_r} - {desc}\n"
                f"Lote: {lote_r}  |  Tipo: {tipo_u}  |  Cantidad: {cant}\n"
                f"Motivo de baja revertida: {motivo}\n"
                f"Neto actual: Pallets={net_p}  Packs={net_pk}"
            )
            self.status_var.set(msg)
            self._refresh_sheet_background()
            self._on_motivo_changed()   # refresca lista por si quedó sin bajas

        except Exception as exc:
            log.exception("Error al realizar reingreso")
            self.status_var.set(f"Error: {exc}")
        finally:
            self.btn.config(state="normal", text="REALIZAR REINGRESO")


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