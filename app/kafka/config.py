"""Configuración de Kafka para el servicio de resumen IA."""

from __future__ import annotations

import os
import ssl
from dataclasses import dataclass
from typing import Optional

from dotenv import load_dotenv

load_dotenv()


@dataclass(frozen=True)
class KafkaSettings:
    bootstrap_servers: str
    topic_resumen_solicitado: str
    topic_resumen_generado: str
    consumer_group_id: str
    client_id: Optional[str] = None
    security_protocol: Optional[str] = None
    sasl_mechanism: Optional[str] = None
    sasl_username: Optional[str] = None
    sasl_password: Optional[str] = None
    ssl_cafile: Optional[str] = None


def _parse_bool(value: Optional[str], default: bool = False) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"true", "1", "yes", "on"}


kafka_settings = KafkaSettings(
    bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
    topic_resumen_solicitado=os.getenv(
        "KAFKA_TOPIC_RESUMEN_SOLICITADO", "resumen-solicitado"
    ),
    topic_resumen_generado=os.getenv(
        "KAFKA_TOPIC_RESUMEN_GENERADO", "resumen-generado"
    ),
    consumer_group_id=os.getenv("KAFKA_CONSUMER_GROUP", "resumen-ia-service"),
    client_id=os.getenv("KAFKA_CLIENT_ID", "resumen-ia-microservice"),
    security_protocol=os.getenv("KAFKA_SECURITY_PROTOCOL"),
    sasl_mechanism=os.getenv("KAFKA_SASL_MECHANISM"),
    sasl_username=os.getenv("KAFKA_SASL_USERNAME"),
    sasl_password=os.getenv("KAFKA_SASL_PASSWORD"),
    ssl_cafile=os.getenv("KAFKA_SSL_CAFILE"),
)

kafka_enabled = _parse_bool(os.getenv("KAFKA_ENABLED"), default=True)

# URLs de los servicios que la IA consulta para enriquecer el resumen.
CLIENTES_URL = os.getenv("CLIENTES_URL", "http://localhost:8082")
VEHICULOS_URL = os.getenv("VEHICULOS_URL", "http://localhost:8083")

# Credencial de ia-resumen ante el orquestador (el API Gateway). Obligatoria si CLIENTES_URL y
# VEHICULOS_URL apuntan al orquestador (<orquestador>/api); vacía si se llama directo a los servicios.
ORQUESTADOR_CLAVE_SERVICIO = os.getenv("ORQUESTADOR_CLAVE_SERVICIO") or None


def headers_de_servicio() -> dict[str, str]:
    """Headers con los que ia-resumen se identifica ante el orquestador (vacío si no hay clave)."""
    if not ORQUESTADOR_CLAVE_SERVICIO:
        return {}
    return {"X-Origen-Servicio": "ia-resumen", "X-Clave-Servicio": ORQUESTADOR_CLAVE_SERVICIO}

# Tópicos a asegurar al arrancar (Aiven no auto-crea tópicos).
kafka_topics = [
    kafka_settings.topic_resumen_solicitado,
    kafka_settings.topic_resumen_generado,
]


def _seguridad() -> dict:
    """Argumentos de seguridad para aiokafka (SASL_SSL de Aiven y similares)."""
    if not kafka_settings.security_protocol:
        return {}
    args = {"security_protocol": kafka_settings.security_protocol}
    if kafka_settings.sasl_mechanism:
        args["sasl_mechanism"] = kafka_settings.sasl_mechanism
    if kafka_settings.sasl_username:
        args["sasl_plain_username"] = kafka_settings.sasl_username
    if kafka_settings.sasl_password:
        args["sasl_plain_password"] = kafka_settings.sasl_password
    if kafka_settings.ssl_cafile:
        contexto = ssl.create_default_context(cafile=kafka_settings.ssl_cafile)
        args["ssl_context"] = contexto
    return args
