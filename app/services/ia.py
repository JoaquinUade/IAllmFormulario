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
            "datos recibidos en JSON, generá un resumen en español en el "
            "siguiente formato exacto (una sola línea por punto, sin "
            "encabezados, viñetas ni texto adicional):\n"
            "\n"
            "1. \"[Nombre] [Apellido] se interesa por un [marca] [modelo] ([anio]).\"\n"
            "2. \"Se encuentra en [provincia], [localidad].\" Omití esta línea si no hay ubicación.\n"
            "3. \"Dispone de anticipo de $[anticipo].\" y/o \"Ofrece permuta: [descripcion de la permuta].\" "
            "Incluí esta línea solo si el dato existe; si no hay anticipo ni permuta, omitila.\n"
            "4. \"Requiere financiación.\" o \"No requiere financiación.\" Incluí esta línea solo si "
            "solicitaFinanciacion está disponible.\n"
            "5. \"Recomendación: GO\" o \"Recomendación: NO GO\".\n"
            "6. \"Mensaje al cliente: ...\" con un mensaje concreto y accionable, de hasta dos líneas, "
            "para iniciar la conversación y cerrar la venta.\n"
            "\n"
            "Reglas:\n"
            "- Usá únicamente los datos recibidos. No inventes información.\n"
            "- No incluyas CUIL, identificadores internos, precios del vehículo ni datos ajenos al formulario.\n"
            "- Criterio GO/NO GO: GO cuando haya señales claras de intención de compra "
            "(anticipo, permuta con valor o solicitaFinanciacion en true). NO GO cuando falten "
            "esos datos o la información sea insuficiente para avanzar.\n"
            "- Los valores opcionales que no estén presentes se omiten en lugar de dejarse vacíos."
        ),
        input=datos_para_ia(datos),
    )
    return limitar_resumen(respuesta.output_text)
