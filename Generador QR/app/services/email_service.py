"""
Servicio de envío de email para el Generador QR.
Se ejecuta en un hilo daemon para no bloquear la respuesta HTTP.
"""
from __future__ import annotations

import logging
import os
import smtplib
import threading
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger("generador_qr")

# ── Configuración desde .env ──────────────────────────────────────────────────
_SMTP_HOST = os.getenv("EMAIL_SMTP_HOST", "smtp.gmail.com")
_SMTP_PORT = int(os.getenv("EMAIL_SMTP_PORT", 587))
_SMTP_USER = os.getenv("EMAIL_SMTP_USER", "")
_SMTP_PASS = os.getenv("EMAIL_SMTP_PASSWORD", "")
_FROM      = os.getenv("EMAIL_FROM", "")
_TO        = os.getenv("EMAIL_TO", "")
_USE_TLS   = os.getenv("EMAIL_USE_TLS", "true").lower() == "true"


def _send(product_id: int, descripcion: str, cantidad: int, ultimo_serie: int, lote: str) -> None:
    """Función interna que envía el email. Llamar siempre desde un hilo."""
    try:
        ahora = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
        primer_serie = ultimo_serie - cantidad + 1

        asunto = f"[QR] PDF generado – Producto {product_id} | Lote {lote}"

        cuerpo_html = f"""
        <html><body style="font-family:Arial,sans-serif;font-size:15px;color:#333;">
          <h2 style="color:#667eea;">&#128203; Nuevo lote de QRs generado</h2>
          <p><strong>Fecha y hora:</strong> {ahora}</p>
          <table border="0" cellpadding="10" cellspacing="0"
                 style="border-collapse:collapse;width:100%;max-width:480px;
                        border:1px solid #ddd;border-radius:8px;">
            <tr style="background:#f0f4ff;">
              <td style="width:160px;"><strong>Producto ID</strong></td>
              <td>{product_id}</td>
            </tr>
            <tr>
              <td><strong>Descripción</strong></td>
              <td>{descripcion}</td>
            </tr>
            <tr style="background:#f0f4ff;">
              <td><strong>Lote</strong></td>
              <td>{lote}</td>
            </tr>
            <tr>
              <td><strong>Cantidad generada</strong></td>
              <td>{cantidad}</td>
            </tr>
            <tr style="background:#f0f4ff;">
              <td><strong>Series generadas</strong></td>
              <td>{primer_serie} → {ultimo_serie}</td>
            </tr>
          </table>
          <p style="margin-top:20px;color:#aaa;font-size:11px;">
            Mensaje automático – Sistema de Generación QR Stock
          </p>
        </body></html>
        """

        msg = MIMEMultipart("alternative")
        msg["Subject"] = asunto
        msg["From"]    = _FROM
        msg["To"]      = _TO
        msg.attach(MIMEText(cuerpo_html, "html", "utf-8"))

        destinatarios = [e.strip() for e in _TO.split(",") if e.strip()]

        if _USE_TLS:
            servidor = smtplib.SMTP(_SMTP_HOST, _SMTP_PORT, timeout=10)
            servidor.ehlo()
            servidor.starttls()
            servidor.ehlo()
        else:
            servidor = smtplib.SMTP_SSL(_SMTP_HOST, _SMTP_PORT, timeout=10)

        servidor.login(_SMTP_USER, _SMTP_PASS)
        servidor.sendmail(_FROM, destinatarios, msg.as_bytes())
        servidor.quit()

        logger.info(f"✉️  Email enviado – Producto {product_id} | Lote {lote} | Series {primer_serie}→{ultimo_serie}")

    except Exception as exc:
        logger.warning(f"⚠️  No se pudo enviar el email: {exc}")


def send_email_async(
    product_id: int,
    descripcion: str,
    cantidad: int,
    ultimo_serie: int,
    lote: str,
) -> None:
    """Lanza el envío de email en un hilo daemon (no bloquea la respuesta HTTP)."""
    threading.Thread(
        target=_send,
        args=(product_id, descripcion, cantidad, ultimo_serie, lote),
        daemon=True,
    ).start()
