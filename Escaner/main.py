from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pathlib import Path
import json
import os
import sys
import smtplib
import logging
import threading
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime

# ==========================================
# RUTAS ABSOLUTAS (evita problemas de CWD)
# ==========================================
BASE_DIR = Path(__file__).resolve().parent
TEMPLATES_DIR = BASE_DIR / "templates"
STATIC_DIR = BASE_DIR / "static"

# Verificar que existen (debug)
if not TEMPLATES_DIR.exists():
    raise RuntimeError(f" Directorio templates no encontrado en: {TEMPLATES_DIR}")
if not (TEMPLATES_DIR / "index.html").exists():
    raise RuntimeError(f" index.html no encontrado en: {TEMPLATES_DIR / 'index.html'}")

print(f"✅ Python: {sys.version}")
print(f"✅ Templates en: {TEMPLATES_DIR}")
print(f"✅ Static en: {STATIC_DIR}")

app = FastAPI(title="Stock QR Scanner")

# Montar archivos estáticos
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

# Inicializar Jinja2 con ruta absoluta
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))
logger = logging.getLogger("stock_qr")


# ── Envío de email ─────────────────────────────────────────────────────────────
def send_email(id_producto: int, nro_serie: int) -> None:
    """Envía un correo con el producto y número de serie escaneado."""
    from config import (
        EMAIL_SMTP_HOST, EMAIL_SMTP_PORT,
        EMAIL_SMTP_USER, EMAIL_SMTP_PASSWORD,
        EMAIL_FROM, EMAIL_TO, EMAIL_USE_TLS,
    )
    try:
        ahora = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
        asunto = f"[Stock] Ingreso QR – Producto {id_producto} | Serie {nro_serie}"

        cuerpo_html = f"""
        <html><body style="font-family:Arial,sans-serif;font-size:15px;color:#333;">
          <h2 style="color:#667eea;">&#128230; Nuevo ingreso de stock registrado</h2>
          <p><strong>Fecha y hora:</strong> {ahora}</p>
          <table border="0" cellpadding="10" cellspacing="0"
                 style="border-collapse:collapse;width:100%;max-width:420px;
                        border:1px solid #ddd;border-radius:8px;">
            <tr style="background:#f0f4ff;">
              <td style="width:140px;"><strong>Producto ID</strong></td>
              <td>{id_producto}</td>
            </tr>
            <tr>
              <td><strong>Número de serie</strong></td>
              <td>{nro_serie}</td>
            </tr>
          </table>
          <p style="margin-top:20px;color:#aaa;font-size:11px;">
            Mensaje automático – Sistema de Escaneo QR Stock
          </p>
        </body></html>
        """

        msg = MIMEMultipart("alternative")
        msg["Subject"] = asunto
        msg["From"]    = EMAIL_FROM
        msg["To"]      = EMAIL_TO
        msg.attach(MIMEText(cuerpo_html, "html", "utf-8"))

        destinatarios = [e.strip() for e in EMAIL_TO.split(",") if e.strip()]

        if EMAIL_USE_TLS:
            servidor = smtplib.SMTP(EMAIL_SMTP_HOST, EMAIL_SMTP_PORT, timeout=10)
            servidor.ehlo()
            servidor.starttls()
            servidor.ehlo()
        else:
            servidor = smtplib.SMTP_SSL(EMAIL_SMTP_HOST, EMAIL_SMTP_PORT, timeout=10)

        servidor.login(EMAIL_SMTP_USER, EMAIL_SMTP_PASSWORD)
        servidor.sendmail(EMAIL_FROM, destinatarios, msg.as_bytes())
        servidor.quit()
        logger.info(f"✉️  Email enviado – Producto {id_producto} | Serie {nro_serie}")

    except Exception as exc:
        logger.warning(f"⚠️  No se pudo enviar el email: {exc}")


@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    """Página principal"""
    # Fix: sintaxis compatible con Starlette moderno
    return templates.TemplateResponse(request, "index.html")

@app.post("/api/scan")
async def scan_qr(request: Request):
    """Endpoint para procesar QR"""
    try:
        # 1. Leer bytes crudos y decodificar de forma segura
        raw_bytes = await request.body()
        raw_str = raw_bytes.decode("utf-8", errors="replace")

        # 2. Parsear JSON
        try:
            payload = json.loads(raw_str)
        except json.JSONDecodeError:
            return JSONResponse(
                status_code=400,
                content={"ok": False, "error": "JSON inválido"}
            )

        raw_qr = payload.get("qr", "")
        if not raw_qr:
            return JSONResponse(
                status_code=400,
                content={"ok": False, "error": "QR vacío"}
            )

        is_complete = payload.get("is_complete", False) is True
        packs = int(payload.get("packs", 0) or 0)

        # 3. Parsear QR
        from utils import parse_qr
        qr_data = parse_qr(raw_qr)

        # 4. Base de datos
        from db import get_db
        from config import DB_SCHEMA, MAX_AUTOFILL

        conn = get_db()
        cur = conn.cursor()
        try:
            # Verificar que el producto existe
            cur.execute(
                f"SELECT 1 FROM {DB_SCHEMA}.productos WHERE id = %s LIMIT 1",
                (qr_data["id_producto"],)
            )
            if not cur.fetchone():
                return JSONResponse(
                    status_code=404,
                    content={"ok": False, "error": f"Producto {qr_data['id_producto']} no existe"}
                )

            # ── Verificar duplicado ──────────────────────────────────────────
            # Duplicado = mismo producto + mismo nro_serie + mismo lote
            # (mismo producto + mismo nro_serie pero distinto lote → permitido)
            cur.execute(
                f"""
                SELECT 1 FROM {DB_SCHEMA}.stock
                WHERE id_producto = %s
                  AND nro_serie   = %s
                  AND lote        = %s
                LIMIT 1
                """,
                (qr_data["id_producto"], qr_data["nro_serie"], qr_data["lote"])
            )
            if cur.fetchone():
                return JSONResponse(
                    status_code=409,
                    content={"ok": False, "error": "DUPLICADO", "data": qr_data}
                )

            # ── Obtener el último nro_serie registrado para este producto+lote ──
            # Se hace ANTES de insertar para calcular el gap correctamente
            cur.execute(
                f"""
                SELECT COALESCE(MAX(nro_serie), 0)
                FROM {DB_SCHEMA}.stock
                WHERE id_producto = %s
                  AND lote        = %s
                """,
                (qr_data["id_producto"], qr_data["lote"])
            )
            last_serie = cur.fetchone()[0]  # 0 si no hay ninguno todavía

            # ── Insertar el registro escaneado ───────────────────────────────
            packs_val = packs if not is_complete else 0
            cur.execute(
                f"""
                INSERT INTO {DB_SCHEMA}.stock
                    (id_producto, nro_serie, lote, creacion, vencimiento, tipo_unidad, packs)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT DO NOTHING
                """,
                (
                    qr_data["id_producto"], qr_data["nro_serie"], qr_data["lote"],
                    qr_data["creacion"], qr_data["vencimiento"], "PALLET", packs_val
                )
            )
            if cur.rowcount == 0:
                return JSONResponse(
                    status_code=500,
                    content={"ok": False, "error": "Conflicto al insertar"}
                )

            # ── Autocompletar series intermedias ────────────────────────────
            # Casos:
            #   last_serie = 0  → primer registro del lote, iterar desde 1 hasta nro_serie - 1
            #   last_serie > 0  → iterar desde (last_serie + 1) hasta (nro_serie - 1)
            autofill_count = 0
            autofill_msg   = "OK"

            inicio = last_serie + 1 if last_serie > 0 else 1

            if qr_data["nro_serie"] > inicio:
                gap = qr_data["nro_serie"] - inicio  # series que faltan

                if gap <= MAX_AUTOFILL:
                    for ns in range(inicio, qr_data["nro_serie"]):
                        cur.execute(
                            f"""
                            INSERT INTO {DB_SCHEMA}.stock
                                (id_producto, nro_serie, lote, creacion, vencimiento, tipo_unidad, packs)
                            VALUES (%s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT DO NOTHING
                            """,
                            (
                                qr_data["id_producto"], ns, qr_data["lote"],
                                qr_data["creacion"], qr_data["vencimiento"], "PALLET", 0
                            )
                        )
                        autofill_count += cur.rowcount

                    autofill_msg = f"Autocompletados {autofill_count} ({inicio}→{qr_data['nro_serie'] - 1})"
                else:
                    autofill_msg = f"⚠️ Gap demasiado grande ({gap}), sin autocompletar"

            # ── Enviar email en segundo plano ──────────────────────────────
            threading.Thread(
                target=send_email,
                args=(qr_data["id_producto"], qr_data["nro_serie"]),
                daemon=True
            ).start()

            return JSONResponse(content={
                "ok":       True,
                "message":  "Registrado correctamente",
                "data":     qr_data,
                "unit":     "COMPLETO" if is_complete else f"PARCIAL ({packs_val})",
                "autofill": autofill_msg
            })

        except Exception as e:
            conn.rollback()
            return JSONResponse(
                status_code=500,
                content={"ok": False, "error": "Error BD", "detail": str(e)}
            )
        finally:
            cur.close()
            conn.close()

    except ValueError as e:
        return JSONResponse(
            status_code=400,
            content={"ok": False, "error": "QR_INVÁLIDO", "message": str(e)}
        )
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"ok": False, "error": "ERROR_INTERNO", "detail": str(e)}
        )


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="info")