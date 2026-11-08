"""Publica el resumen generado por la IA hacia el topic `resumen-generado`."""

from __future__ import annotations

import json
import logging
import uuid
from datetime import datetime, timezone
from typing import Any, Optional

from aiokafka import AIOKafkaProducer

from app.kafka.config import kafka_enabled, kafka_settings, _seguridad

logger = logging.getLogger(__name__)

_producer: Optional[AIOKafkaProducer] = None


def build_event_envelope(
    event_type: str,
    payload: dict[str, Any],
    correlation_id: Optional[str] = None,
) -> dict[str, Any]:
    return {
        "eventId": str(uuid.uuid4()),
        "eventType": event_type,
        "correlationId": correlation_id or str(uuid.uuid4()),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "payload": payload,
    }


async def get_producer() -> Optional[AIOKafkaProducer]:
    global _producer
    if not kafka_enabled:
        return None
    if _producer is None:
        _producer = AIOKafkaProducer(
            bootstrap_servers=kafka_settings.bootstrap_servers,
            client_id=kafka_settings.client_id,
            value_serializer=lambda value: json.dumps(value, default=str).encode("utf-8"),
            **_seguridad(),
        )
        await _producer.start()
    return _producer


async def publish_resumen_generado(
    lead_id: str,
    resumen: str,
    correlation_id: Optional[str] = None,
) -> None:
    """Publica ResumenGenerado con el texto del resumen comercial."""
    producer = await get_producer()
    if producer is None:
        logger.info(
            "Kafka deshabilitado: resumen de lead %s no publicado.", lead_id
        )
        return

    message = build_event_envelope(
        "ResumenGenerado",
        {"leadId": lead_id, "resumen": resumen},
        correlation_id,
    )
    await producer.send_and_wait(
        kafka_settings.topic_resumen_generado,
        key=lead_id.encode("utf-8"),
        value=message,
    )


async def stop_producer() -> None:
    global _producer
    if _producer is not None:
        await _producer.stop()
        _producer = None
