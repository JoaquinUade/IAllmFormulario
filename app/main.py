from __future__ import annotations

import os
from typing import Annotated

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, ConfigDict, Field


app = FastAPI(
    title="Resumen comercial de clientes",
    description="Genera un resumen breve de una oportunidad de compra de vehículo.",
    version="1.0.0",
)


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


def datos_para_ia(datos: ClientePotencial) -> str:
    """Serializa únicamente los campos comerciales permitidos."""
    return datos.model_dump_json()


def limitar_resumen(resumen: str) -> str:
    lineas = [linea.strip() for linea in resumen.splitlines() if linea.strip()]
    if not lineas:
        raise HTTPException(
            status_code=502,
            detail="La IA devolvió un resumen vacío.",
        )
    return "\n".join(lineas[:5])


def formato_pesos(valor: int) -> str:
    return f"${valor:,}".replace(",", ".")


def texto_ubicacion(ubicacion: Ubicacion | None) -> str:
    if ubicacion is None:
        return "No informada"

    partes = [
        parte.replace("_", " ")
        for parte in (ubicacion.localidad, ubicacion.provincia)
        if parte
    ]
    return ", ".join(partes) if partes else "No informada"


def texto_permuta(permuta: Permuta | None) -> str:
    if permuta is None:
        return "No informada"
    if not permuta.tienePermuta:
        return "No"

    detalles = ["Sí"]
    if permuta.descripcion:
        detalles.append(permuta.descripcion)
    if permuta.valorDeclarado is not None:
        detalles.append(f"valor declarado {formato_pesos(permuta.valorDeclarado)}")
    return ", ".join(detalles)


def generar_resumen(datos: ClientePotencial) -> str:
    nombre = f"{datos.cliente.nombre} {datos.cliente.apellido}".strip()
    vehiculo = (
        f"{datos.vehiculo.marca} {datos.vehiculo.modelo} "
        f"({datos.vehiculo.anio})"
    )
    anticipo = (
        formato_pesos(datos.anticipo)
        if datos.anticipo is not None
        else "No informado"
    )
    financiacion = (
        "Sí"
        if datos.solicitaFinanciacion is True
        else "No"
        if datos.solicitaFinanciacion is False
        else "No informada"
    )

    conclusion = (
        "El asesor debería validar la tasación de la permuta y revisar las "
        "condiciones de financiación solicitadas."
        if datos.permuta is not None and datos.permuta.tienePermuta
        and datos.solicitaFinanciacion is True
        else "El asesor debería confirmar los datos comerciales y las "
        "condiciones de la operación."
    )

    return "\n".join(
        [
            f"Cliente: {nombre}.",
            f"Vehículo seleccionado: {vehiculo}.",
            f"Ubicación: {texto_ubicacion(datos.ubicacion)}. Anticipo: {anticipo}.",
            f"Permuta: {texto_permuta(datos.permuta)}. Financiación: {financiacion}.",
            f"Conclusión comercial: {conclusion}",
        ]
    )


def generar_resumen_con_ia(datos: ClientePotencial) -> str:
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise HTTPException(
            status_code=500,
            detail="La variable OPENAI_API_KEY no está configurada.",
        )

    try:
        from openai import OpenAI
    except ImportError as error:
        raise HTTPException(
            status_code=500,
            detail="La dependencia de IA no está instalada.",
        ) from error

    cliente_ia = OpenAI(api_key=api_key)
    respuesta = cliente_ia.responses.create(
        model=os.getenv("OPENAI_MODEL", "gpt-4o-mini"),
        instructions=(
            "Sos un asesor comercial de concesionaria. Generá un resumen "
            "profesional en español, de hasta cinco líneas. Usá únicamente "
            "los datos recibidos. No inventes información, no incluyas CUIL, "
            "identificadores internos, precios del vehículo ni datos ajenos "
            "al formulario permitido."
            "Por ultimo incluime un mensaje de hasta dos lineas con un mensaje recomendado para encarar al cliente y cerrar la venta, el cual empieza con mensaje:."
        ),
        input=datos_para_ia(datos),
    )
    return limitar_resumen(respuesta.output_text)


@app.post("/resumen", response_model=ResumenResponse)
def crear_resumen(datos: ClientePotencial) -> ResumenResponse:
    return ResumenResponse(resumen=generar_resumen(datos))


@app.post("/resumen-ia", response_model=ResumenResponse)
def crear_resumen_ia(datos: ClientePotencial) -> ResumenResponse:
    return ResumenResponse(resumen=generar_resumen_con_ia(datos))
