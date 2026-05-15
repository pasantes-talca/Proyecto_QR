"""
Router: Caché de series

GET    /api/cache/{product_id}  →  Consulta el estado de caché de un producto
DELETE /api/cache/{product_id}  →  Resetea el N° de serie a 0
"""
from fastapi import APIRouter

from app.schemas.models import CacheState
from app.services import cache_service

router = APIRouter(prefix="/api/cache", tags=["Caché"])


@router.get(
    "/{product_id}",
    response_model=CacheState,
    summary="Consultar caché de serie",
    description="Devuelve el último N° de serie generado y el próximo que se usará.",
)
def get_cache(product_id: int) -> CacheState:
    last = cache_service.get_serie(product_id)
    return CacheState(
        product_id=product_id,
        last_serie=last,
        next_serie=last + 1,
    )


@router.delete(
    "/{product_id}",
    response_model=CacheState,
    summary="Resetear caché de serie",
    description="Resetea el contador a 0. La próxima impresión arrancará desde N° de serie 1.",
)
def reset_cache(product_id: int) -> CacheState:
    cache_service.reset_serie(product_id)
    return CacheState(
        product_id=product_id,
        last_serie=0,
        next_serie=1,
    )