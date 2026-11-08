"""Servicio que genera el resumen comercial mediante un modelo de IA."""

from __future__ import annotations

import os

from fastapi import HTTPException

from app.schemas import ClientePotencial


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


def generar_resumen_con_ia(datos: ClientePotencial) -> str:
    api_key = os.getenv("OLLAMA_API_KEY")
    if not api_key:
        raise HTTPException(
            status_code=500,
            detail="La variable OLLAMA_API_KEY no está configurada.",
        )

    try:
        from openai import OpenAI
    except ImportError as error:
        raise HTTPException(
            status_code=500,
            detail="La dependencia de IA no está instalada.",
        ) from error

    cliente_ia = OpenAI(
        base_url=os.getenv("OLLAMA_BASE_URL", "https://ollama.com/v1"),
        api_key=api_key,
    )
    respuesta = cliente_ia.responses.create(
        model=os.getenv("OLLAMA_MODEL", "nemotron-3-super:cloud"),
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
