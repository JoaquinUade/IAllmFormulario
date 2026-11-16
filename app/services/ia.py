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
    return "\n".join(lineas[:6])


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
            "Actuá como asesor comercial de una concesionaria. A partir de los "
            "datos recibidos en JSON, redactá un único párrafo en español, en "
            "lenguaje natural y fluido, sin encabezados, viñetas, números ni "
            "saltos de línea. El párrafo debe contener, en este orden, "
            "únicamente los datos disponibles:\n"
            "- Apertura: \"[Nombre] [Apellido] se interesa por un [marca] [modelo] ([anio])\".\n"
            "- Ubicación: \"Se encuentra en [provincia], [localidad]\" (solo si hay ubicación).\n"
            "- Anticipo: \"dispone de un anticipo de $[anticipo]\" y/o permuta: \"ofrece permuta "
            "de [descripcion de la permuta]\" (solo si existen).\n"
            "- Financiación: \"requiere financiación\" o \"no requiere financiación\" "
            "(solo si solicitaFinanciacion está disponible).\n"
            "- Recomendación: \"Recomendación: GO\" o \"Recomendación: NO GO\".\n"
            "- Cierre: un mensaje concreto y accionable, de hasta dos líneas, para iniciar "
            "la conversación con el cliente y cerrar la venta, introducido como "
            "\"Mensaje al cliente: ...\".\n"
            "\n"
            "Reglas:\n"
            "- Usá únicamente los datos recibidos. No inventes información.\n"
            "- No incluyas CUIL, identificadores internos, precios del vehículo ni datos ajenos al formulario.\n"
            "- Criterio GO/NO GO: GO cuando haya señales claras de intención de compra "
            "(anticipo, permuta con valor o solicitaFinanciacion en true). NO GO cuando falten "
            "esos datos o la información sea insuficiente para avanzar.\n"
            "- Los valores opcionales que no estén presentes se omiten en lugar de dejarse vacíos.\n"
            "- Uní todo el contenido en un solo párrafo continuo, sin puntos y aparte ni saltos de línea."
        ),
        input=datos_para_ia(datos),
    )
    return limitar_resumen(respuesta.output_text)
