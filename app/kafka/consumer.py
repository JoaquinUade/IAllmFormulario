"""Consumidor de `resumen-solicitado` que genera el resumen con IA.

Por cada solicitud, enriquece los datos del lead consultando `clientes` y
`vehiculos` por REST (nombre/apellido y marca/modelo/anio), arma el
`ClientePotencial`, llama a `generar_resumen_con_ia` y publica el resultado en
`resumen-generado`.
"""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Optional

import httpx
from aiokafka import AIOKafkaConsumer
from aiokafka.admin import AIOKafkaAdminClient, NewTopic

from app.kafka.config import (
    CLIENTES_URL,
    VEHICULOS_URL,
    kafka_enabled,
    kafka_settings,
    kafka_topics,
    _seguridad,
)
from app.kafka.producer import publish_resumen_generado
from app.schemas import ClientePotencial
from app.services.ia import generar_resumen_con_ia

logger = logging.getLogger(__name__)

_consumer: Optional[AIOKafkaConsumer] = None
_consumer_task: Optional[asyncio.Task] = None

_HTTP_TIMEOUT = 5.0


def _a_int(valor: Optional[object]) -> Optional[int]:
    if valor is None:
        return None
    try:
        return int(valor)
    except (TypeError, ValueError):
        return None


def _armar_cliente_potencial(payload: dict, cliente: dict, vehiculo: dict) -> ClientePotencial:
    permuta = payload.get("permuta") or {}
    ubicacion = payload.get("ubicacion") or {}

    return ClientePotencial(
        cliente={
            "nombre": cliente.get("nombre"),
            "apellido": cliente.get("apellido"),
        },
        vehiculo={
            "marca": vehiculo.get("marca"),
            "modelo": vehiculo.get("modelo"),
            "anio": vehiculo.get("anio"),
        },
        anticipo=_a_int(payload.get("anticipo")),
        permuta={
            "tienePermuta": bool(permuta.get("tienePermuta", False)),
            "descripcion": permuta.get("descripcion"),
            "valorDeclarado": _a_int(permuta.get("valorDeclarado")),
        },
        ubicacion={
            "provincia": ubicacion.get("provincia"),
            "localidad": ubicacion.get("localidad"),
        },
        solicitaFinanciacion=payload.get("solicitaFinanciacion"),
    )


async def _enriquecer(payload: dict) -> ClientePotencial:
    cliente_id = payload.get("clienteId")
    vehiculo_id = payload.get("vehiculoId")

    async with httpx.AsyncClient(timeout=_HTTP_TIMEOUT) as client:
        cliente: dict = {}
        if cliente_id:
            try:
                resp = await client.get(f"{CLIENTES_URL}/clientes/{cliente_id}")
                resp.raise_for_status()
                cliente = resp.json()
            except (httpx.HTTPError, ValueError) as exc:
                logger.warning("No se pudo obtener cliente %s: %s", cliente_id, exc)

        vehiculo: dict = {}
        if vehiculo_id:
            try:
                resp = await client.get(f"{VEHICULOS_URL}/vehiculos/{vehiculo_id}")
                resp.raise_for_status()
                vehiculo = resp.json()
            except (httpx.HTTPError, ValueError) as exc:
                logger.warning("No se pudo obtener vehículo %s: %s", vehiculo_id, exc)

    return _armar_cliente_potencial(payload, cliente, vehiculo)


async def handle_resumen_solicitado(message_value: dict) -> None:
    payload = message_value.get("payload", message_value)
    lead_id = payload.get("leadId")
    correlation_id = message_value.get("correlationId")

    logger.info("Resumen solicitado: leadId=%s", lead_id)

    datos = await _enriquecer(payload)
    # La llamada a Ollama es bloqueante; se corre en un thread para no bloquear el loop.
    resumen = await asyncio.to_thread(generar_resumen_con_ia, datos)

    await publish_resumen_generado(
        lead_id=str(lead_id),
        resumen=resumen,
        correlation_id=correlation_id,
    )
    logger.info("Resumen publicado para leadId=%s", lead_id)


async def _asegurar_topicos() -> None:
    try:
        admin = AIOKafkaAdminClient(
            bootstrap_servers=kafka_settings.bootstrap_servers,
            client_id=f"{kafka_settings.client_id}-admin",
            **_seguridad(),
        )
        await admin.start()
        try:
            existentes = set(await admin.list_topics())
            nuevos = [
                NewTopic(name=nombre, num_partitions=3, replication_factor=1)
                for nombre in kafka_topics
                if nombre not in existentes
            ]
            if nuevos:
                await admin.create_topics(new_topics=nuevos)
                logger.info("Tópicos creados: %s", [n.name for n in nuevos])
        finally:
            await admin.close()
    except Exception as exc:
        logger.warning("No se pudieron asegurar los tópicos de Kafka: %s", exc)


async def consume_messages(consumer: AIOKafkaConsumer) -> None:
    try:
        async for message in consumer:
            try:
                value = json.loads(message.value.decode("utf-8"))
                await handle_resumen_solicitado(value)
            except json.JSONDecodeError as exc:
                logger.error("Mensaje con JSON inválido: %s", exc)
            except Exception as exc:
                logger.exception("Error procesando resumen-solicitado: %s", exc)
    finally:
        await consumer.stop()


async def start_consumer() -> None:
    global _consumer, _consumer_task
    if not kafka_enabled:
        logger.info("Kafka deshabilitado: consumer no iniciado.")
        return
    if _consumer is not None:
        return

    await _asegurar_topicos()

    _consumer = AIOKafkaConsumer(
        kafka_settings.topic_resumen_solicitado,
        bootstrap_servers=kafka_settings.bootstrap_servers,
        group_id=kafka_settings.consumer_group_id,
        client_id=kafka_settings.client_id,
        auto_offset_reset="earliest",
        value_deserializer=lambda value: value,
        **_seguridad(),
    )
    await _consumer.start()
    logger.info(
        "Consumer Kafka iniciado: topic=%s group_id=%s",
        kafka_settings.topic_resumen_solicitado,
        kafka_settings.consumer_group_id,
    )

    _consumer_task = asyncio.create_task(consume_messages(_consumer))


async def stop_consumer() -> None:
    global _consumer, _consumer_task
    if _consumer is not None:
        await _consumer.stop()
        _consumer = None
    if _consumer_task is not None:
        _consumer_task.cancel()
        try:
            await _consumer_task
        except Exception:
            pass
        _consumer_task = None
