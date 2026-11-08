"""Modelos Pydantic que definen el contrato de entrada y salida de la API."""

from __future__ import annotations

from typing import Annotated

from pydantic import BaseModel, ConfigDict, Field


class Cliente(BaseModel):
    model_config = ConfigDict(extra="ignore")

    nombre: str = Field(min_length=1)
    apellido: str = Field(min_length=1)


class Vehiculo(BaseModel):
    model_config = ConfigDict(extra="ignore")

    marca: str = Field(min_length=1)
    modelo: str = Field(min_length=1)
    anio: Annotated[int, Field(ge=1886, le=2100)]


class Permuta(BaseModel):
    model_config = ConfigDict(extra="ignore")

    tienePermuta: bool
    descripcion: str | None = None
    valorDeclarado: Annotated[int, Field(ge=0)] | None = None


class Ubicacion(BaseModel):
    model_config = ConfigDict(extra="ignore")

    provincia: str | None = None
    localidad: str | None = None


class ClientePotencial(BaseModel):
    model_config = ConfigDict(extra="ignore")

    cliente: Cliente
    vehiculo: Vehiculo
    anticipo: Annotated[int, Field(ge=0)] | None = None
    permuta: Permuta | None = None
    ubicacion: Ubicacion | None = None
    solicitaFinanciacion: bool | None = None


class ResumenResponse(BaseModel):
    resumen: str
