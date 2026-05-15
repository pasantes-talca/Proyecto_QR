"""
Router: Generación de QRs

POST /api/qr/generate  →  Genera el PDF y lo devuelve como descarga
"""
from fastapi import APIRouter, HTTPException
from fastapi.responses import Response

from app.schemas.models import GenerateRequest
from app.services.pdf_service import generate_pdf
from app.services.email_service import send_email_async

router = APIRouter(prefix="/api/qr", tags=["Generación QR"])


@router.post(
    "/generate",
    summary="Generar PDF con QR codes",
    description=(
        "Genera un PDF con los QR codes para el producto indicado. "
        "Cada N° de serie se imprime dos veces (2 copias). "
        "El PDF se devuelve como descarga directa (application/pdf)."
    ),
    responses={
        200: {
            "content": {"application/pdf": {}},
            "description": "PDF generado exitosamente.",
        }
    },
)
def generate_qr_pdf(req: GenerateRequest) -> Response:
    try:
        pdf_bytes, last_serie, lote = generate_pdf(
            product_id=req.product_id,
            descripcion=req.descripcion,
            cantidad=req.cantidad,
        )
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Error al generar el PDF: {exc}",
        )

    # ── Notificación por email (no bloquea la descarga) ───────────────────────
    send_email_async(
        product_id=req.product_id,
        descripcion=req.descripcion,
        cantidad=req.cantidad,
        ultimo_serie=last_serie,
        lote=lote,
    )

    filename = f"qr_lote_{lote}.pdf"

    return Response(
        content=pdf_bytes,
        media_type="application/pdf",
        headers={
            "Content-Disposition": f'attachment; filename="{filename}"',
            # Headers informativos para el frontend
            "X-Last-Serie": str(last_serie),
            "X-Lote": lote,
            "X-Filename": filename,
            "Access-Control-Expose-Headers": "X-Last-Serie, X-Lote, X-Filename",
        },
    )