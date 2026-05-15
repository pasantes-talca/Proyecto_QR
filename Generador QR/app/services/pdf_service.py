"""
Servicio de generación de PDF con QR codes.

Lógica idéntica a la versión original (Tkinter), adaptada para
devolver los bytes del PDF en memoria en lugar de guardarlo en disco.

Layout:
  - 4 posiciones por página (A4 vertical)
  - 2 copias por N° de serie (mismo QR impreso dos veces consecutivas)
  - QR a la izquierda, texto a la derecha
  - Payload QR: NS=|PRD=|DSC=|LOT=|FEC=|VTO=
"""
from __future__ import annotations

import io
import os
import tempfile
import textwrap
from datetime import datetime

from dateutil.relativedelta import relativedelta
import qrcode
from reportlab.lib.pagesizes import A4
from reportlab.pdfgen import canvas as rl_canvas

from app.services.cache_service import get_serie, set_serie


# ──────────────────────────────────────────────
#  Helpers
# ──────────────────────────────────────────────

def _wrap(text: str, max_chars: int) -> list[str]:
    return textwrap.wrap(str(text), width=max_chars)


def _sanitize_desc(descripcion: str, max_len: int = 90) -> str:
    clean = str(descripcion).replace("\n", " ").replace("|", "/").replace("=", "-").strip()
    return clean[:max_len]


# ──────────────────────────────────────────────
#  Función principal
# ──────────────────────────────────────────────

def generate_pdf(
    product_id: int,
    descripcion: str,
    cantidad: int,
) -> tuple[bytes, int, str]:
    """
    Genera el PDF con los QR codes y devuelve sus bytes en memoria.

    Args:
        product_id:   ID del producto.
        descripcion:  Descripción visible en el QR.
        cantidad:     Cantidad de N° de serie a generar.

    Returns:
        Tuple (pdf_bytes, ultimo_nro_serie, numero_lote)
    """
    # ── Fechas ──────────────────────────────────────────────────────────────
    now = datetime.now()
    numero_lote = now.strftime("%d%m%y")
    fec_iso = now.strftime("%Y-%m-%d")
    vto_iso = (now + relativedelta(months=6)).strftime("%Y-%m-%d")
    fecha_str = now.strftime("%d/%m/%y")
    fecha_venc_str = (now + relativedelta(months=6)).strftime("%d/%m/%y")

    # ── Serie: arranca desde caché ───────────────────────────────────────────
    nro_serie = get_serie(product_id)
    desc_clean = _sanitize_desc(descripcion)

    # ── Canvas en memoria ────────────────────────────────────────────────────
    buffer = io.BytesIO()
    c = rl_canvas.Canvas(buffer, pagesize=A4)
    _, alto = A4

    # 4 posiciones verticales por página
    y_positions = [alto - 230, alto - 430, alto - 630, alto - 830]
    x_qr = 40
    qr_size = 215
    text_x = x_qr + qr_size + 40
    posicion_actual = 0

    # ── Generación de QRs ────────────────────────────────────────────────────
    with tempfile.TemporaryDirectory() as tmpdir:
        for _ in range(cantidad):
            nro_serie += 1

            payload_qr = (
                f"NS={nro_serie:06d}"
                f"|PRD={product_id}"
                f"|DSC={desc_clean}"
                f"|LOT={numero_lote}"
                f"|FEC={fec_iso}"
                f"|VTO={vto_iso}"
            )

            qr_img = qrcode.make(payload_qr)
            qr_path = os.path.join(
                tmpdir,
                f"tmp_qr_{product_id}_{numero_lote}_{nro_serie}.png",
            )
            qr_img.save(qr_path)

            # 2 copias del mismo QR por N° de serie
            for _ in range(2):
                y = y_positions[posicion_actual]
                c.drawImage(qr_path, x_qr, y, width=qr_size, height=qr_size)

                titulo_lineas = _wrap(descripcion, 40)
                resto_lineas = [
                    f"N° de serie: {nro_serie}",
                    f"ID producto: {product_id}",
                    f"Lote: {numero_lote}",
                    f"Creación: {fecha_str}",
                    f"Vencimiento: {fecha_venc_str}",
                ]

                titulo_height = len(titulo_lineas) * 18
                resto_height = len(resto_lineas) * 15
                total_height = titulo_height + resto_height

                centro_qr_y = y + qr_size / 2
                text_y = centro_qr_y + total_height / 2

                # Título en negrita 15pt
                c.setFont("Helvetica-Bold", 15)
                for i, linea_txt in enumerate(titulo_lineas):
                    c.drawString(text_x, text_y - i * 18, linea_txt)

                offset = titulo_height

                # Primer dato (N° de serie) en 18pt destacado
                c.setFont("Helvetica-Bold", 18)
                c.drawString(text_x, text_y - offset, resto_lineas[0])
                offset += 20

                # Resto de datos en 15pt regular
                c.setFont("Helvetica", 15)
                for linea_txt in resto_lineas[1:]:
                    c.drawString(text_x, text_y - offset, linea_txt)
                    offset += 15

                posicion_actual += 1
                if posicion_actual == 4:
                    c.showPage()
                    posicion_actual = 0

    # ── Guardar PDF y persistir caché ────────────────────────────────────────
    c.save()
    set_serie(product_id, nro_serie)

    buffer.seek(0)
    return buffer.read(), nro_serie, numero_lote