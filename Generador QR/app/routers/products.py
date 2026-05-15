"""
Router: Productos

GET /api/products  →  Lista todos los productos de produccion.productos
"""
from fastapi import APIRouter, HTTPException

from app.schemas.models import Product
from app.services.db_service import DBUnavailableError, fetch_products

router = APIRouter(prefix="/api/products", tags=["Productos"])


@router.get(
    "/",
    response_model=list[Product],
    summary="Listar productos",
    description="Devuelve todos los productos de la tabla produccion.productos, ordenados alfabéticamente.",
)
def list_products() -> list[Product]:
    try:
        rows = fetch_products()
    except DBUnavailableError as exc:
        raise HTTPException(status_code=503, detail=str(exc))
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Error inesperado al consultar la base de datos: {exc}",
        )

    return [Product(id=row[0], descripcion=row[1]) for row in rows]