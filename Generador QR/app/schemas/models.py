"""
Modelos de datos (schemas) usados en requests y responses de la API.
"""
from pydantic import BaseModel, Field


class Product(BaseModel):
    id: int
    descripcion: str


class CacheState(BaseModel):
    product_id: int
    last_serie: int = Field(description="Último N° de serie impreso (0 si nunca se imprimió)")
    next_serie: int = Field(description="N° de serie con el que arrancará la próxima impresión")


class GenerateRequest(BaseModel):
    product_id: int = Field(gt=0, description="ID del producto en produccion.productos")
    descripcion: str = Field(min_length=1, description="Descripción del producto")
    cantidad: int = Field(gt=0, le=500, description="Cantidad de N° de serie a generar (máx. 500)")


class GenerateResult(BaseModel):
    last_serie: int
    lote: str
    filename: str
    cantidad: int