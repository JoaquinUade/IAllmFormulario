"""Endpoints de resumen comercial."""

from __future__ import annotations

from fastapi import APIRouter

from app.schemas import ClientePotencial, ResumenResponse
from app.services.ia import generar_resumen_con_ia
from app.services.resumen import generar_resumen

router = APIRouter()


@router.post("/resumen", response_model=ResumenResponse)
def crear_resumen(datos: ClientePotencial) -> ResumenResponse:
    return ResumenResponse(resumen=generar_resumen(datos))


@router.post("/resumen-ia", response_model=ResumenResponse)
def crear_resumen_ia(datos: ClientePotencial) -> ResumenResponse:
    return ResumenResponse(resumen=generar_resumen_con_ia(datos))
