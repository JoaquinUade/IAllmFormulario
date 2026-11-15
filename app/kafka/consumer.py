"""Consumidor del comando `GenerarResumen` (en `crm.comandos`, con `destino: ia-resumen`).

Por cada comando, enriquece los datos del lead consultando `clientes` y
`vehiculos` por REST (nombre/apellido y marca/modelo/anio), arma el
`ClientePotencial`, llama a `generar_resumen_con_ia` y publica el evento
`ResumenGenerado` en `crm.eventos`. Los comandos de otros servicios se ignoran;
el que agota sus reintentos se manda a `crm.dlt`.
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
    SERVICIO,
    espera_reintento_segundos,
    reintentos_comando,
    CLIENTES_URL,
    VEHICULOS_URL,
    headers_de_servicio,
    kafka_enabled,
    kafka_settings,
    kafka_topics,
    _seguridad,
)
from app.kafka.producer import publish_dlt, publish_resumen_generado
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

    async with httpx.AsyncClient(timeout=_HTTP_TIMEOUT, headers=headers_de_servicio()) as client:
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


async def handle_generar_resumen(message_value: dict) -> None:
    payload = message_value.get("payload", message_value)
    lead_id = payload.get("leadId")
    correlation_id = message_value.get("correlationId")

    logger.info("Resumen a generar: leadId=%s", lead_id)

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


async def _despachar(message_value: dict) -> None:
    """Atiende el comando si es para ia-resumen; ignora los de otros servicios y los que no conoce."""
    if message_value.get("destino") != SERVICIO:
        return
    if message_value.get("eventType") != "GenerarResumen":
        logger.warning("Comando desconocido para ia-resumen: %s", message_value.get("eventType"))
        return
    await handle_generar_resumen(message_value)


async def _procesar_con_reintentos(message) -> None:
    """Reintenta un comando que falla; si agota los intentos lo manda a `crm.dlt` y sigue con el siguiente."""
    try:
        value = json.loads(message.value.decode("utf-8"))
    except json.JSONDecodeError as exc:
        logger.error("Mensaje con JSON inválido en %s: %s", message.topic, exc)
        return
    if not isinstance(value, dict) or value.get("destino") != SERVICIO:
        return  # no es para ia-resumen: sin trabajo ni logs de ruido
    espera = espera_reintento_segundos
    for intento in range(1, reintentos_comando + 1):
        try:
            await _despachar(value)
            return
        except Exception as exc:
            logger.warning(
                "Falló %s (intento %s/%s): %s", value.get("eventType"), intento, reintentos_comando, exc
            )
            if intento < reintentos_comando:
                await asyncio.sleep(espera)
                espera *= 2
    logger.error("Comando %s agotó sus reintentos: se envía a la DLT.", value.get("eventType"))
    try:
        await publish_dlt(message.value, message.key)
    except Exception as exc:
        logger.exception("No se pudo enviar el comando a la DLT: %s", exc)


async def consume_messages(consumer: AIOKafkaConsumer) -> None:
    try:
        async for message in consumer:
            await _procesar_con_reintentos(message)
    finally:
        await consumer.stop()


async def start_consumer() -> None:
    global _consumer, _consumer_task
    if not kafka_enabled:
        logger.info("Kafka deshabilitado: consumer no iniciado.")
        return
    if _consumer is not None:
        return

    try:
        await _asegurar_topicos()

        _consumer = AIOKafkaConsumer(
            kafka_settings.topic_comandos,
            bootstrap_servers=kafka_settings.bootstrap_servers,
            group_id=kafka_settings.consumer_group_id,
            client_id=kafka_settings.client_id,
            auto_offset_reset="earliest",
            value_deserializer=lambda value: value,
            **_seguridad(),
        )
        await _consumer.start()
    except Exception as exc:
        # Si Kafka no está disponible, la API sigue levantando; solo queda
        # desactivado el consumidor de resúmenes. Sin esto, el fallo de conexión
        # tumba todo el servicio en el arranque.
        logger.error(
            "No se pudo iniciar el consumer de Kafka (la API sigue operativa): %s",
            exc,
        )
        if _consumer is not None:
            _consumer = None
        return

    logger.info(
        "Consumer Kafka iniciado: topic=%s group_id=%s",
        kafka_settings.topic_comandos,
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
