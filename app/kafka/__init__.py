"""Integración con Kafka del servicio de resumen IA."""

from app.kafka.config import kafka_settings
from app.kafka.consumer import start_consumer, stop_consumer
from app.kafka.producer import publish_resumen_generado, stop_producer

__all__ = [
    "kafka_settings",
    "publish_resumen_generado",
    "start_consumer",
    "stop_consumer",
    "stop_producer",
]
