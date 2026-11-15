"""ia-resumen atiende solo `GenerarResumen` con destino `ia-resumen`, en `crm.comandos`.

Contrato: docs/08-contrato-tres-topicos.md del orquestador. Publica `ResumenGenerado` en `crm.eventos`
y manda a `crm.dlt` el comando que agota sus reintentos.
"""

from __future__ import annotations

import asyncio
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from app.kafka import consumer, producer
from app.kafka.config import kafka_settings

PAYLOAD = {
    "leadId": "L1",
    "clienteId": "C1",
    "vehiculoId": "V1",
    "anticipo": 1000,
    "solicitaFinanciacion": True,
    "permuta": {"tienePermuta": False, "descripcion": None, "valorDeclarado": None},
    "ubicacion": {"provincia": "Córdoba", "localidad": None},
}


def comando(event_type="GenerarResumen", destino="ia-resumen", payload=PAYLOAD) -> SimpleNamespace:
    sobre = {
        "eventId": "e1",
        "eventType": event_type,
        "version": 1,
        "correlationId": "cid-1",
        "destino": destino,
        "payload": payload,
    }
    return SimpleNamespace(topic="crm.comandos", key=b"L1", value=json.dumps(sobre).encode("utf-8"))


@pytest.fixture
def doble(monkeypatch):
    """Reemplaza las llamadas externas: clientes/vehiculos, el modelo de IA, y la publicación en Kafka."""
    cliente = {"nombre": "Ana", "apellido": "Pérez"}
    vehiculo = {"marca": "Toyota", "modelo": "Corolla", "anio": 2022}
    monkeypatch.setattr(consumer, "_enriquecer", AsyncMock(side_effect=lambda p: consumer._armar_cliente_potencial(p, cliente, vehiculo)))
    monkeypatch.setattr(consumer, "generar_resumen_con_ia", lambda datos: "Resumen de prueba")
    monkeypatch.setattr(consumer, "publish_resumen_generado", AsyncMock())
    monkeypatch.setattr(consumer, "publish_dlt", AsyncMock())
    monkeypatch.setattr(consumer, "espera_reintento_segundos", 0.001)
    return SimpleNamespace(generado=consumer.publish_resumen_generado, dlt=consumer.publish_dlt)


def procesar(mensaje) -> None:
    asyncio.run(consumer._procesar_con_reintentos(mensaje))


def test_genera_el_resumen_y_lo_publica_con_el_correlation_id_del_comando(doble) -> None:
    procesar(comando())

    doble.generado.assert_awaited_once_with(lead_id="L1", resumen="Resumen de prueba", correlation_id="cid-1")
    doble.dlt.assert_not_awaited()


@pytest.mark.parametrize("destino", ["leads", "clientes", "calificacion", None])
def test_ignora_los_comandos_de_otros_servicios(doble, destino) -> None:
    procesar(comando(destino=destino))

    doble.generado.assert_not_awaited()
    doble.dlt.assert_not_awaited()


def test_ignora_un_comando_desconocido_para_ia_resumen(doble) -> None:
    procesar(comando(event_type="OtraCosa"))

    doble.generado.assert_not_awaited()
    doble.dlt.assert_not_awaited()


def test_ignora_un_mensaje_que_no_es_json(doble) -> None:
    procesar(SimpleNamespace(topic="crm.comandos", key=None, value=b"no es json"))

    doble.generado.assert_not_awaited()
    doble.dlt.assert_not_awaited()


def test_una_falla_transitoria_se_reintenta_y_no_va_a_la_dlt(doble, monkeypatch) -> None:
    llamadas = []

    def inestable(datos):
        llamadas.append(1)
        if len(llamadas) == 1:
            raise RuntimeError("el modelo no respondió")
        return "Resumen de prueba"

    monkeypatch.setattr(consumer, "generar_resumen_con_ia", inestable)

    procesar(comando())

    assert len(llamadas) == 2
    doble.generado.assert_awaited_once()
    doble.dlt.assert_not_awaited()


def test_al_agotar_los_reintentos_manda_el_comando_original_a_la_dlt(doble, monkeypatch) -> None:
    def siempre_falla(datos):
        raise RuntimeError("el modelo no respondió")

    monkeypatch.setattr(consumer, "generar_resumen_con_ia", siempre_falla)
    mensaje = comando()

    procesar(mensaje)

    doble.generado.assert_not_awaited()
    doble.dlt.assert_awaited_once_with(mensaje.value, mensaje.key)


# ------------------------------------------------- lo que publica


def test_el_resumen_generado_es_un_evento_con_sobre_completo() -> None:
    sobre = producer.build_event_envelope("ResumenGenerado", {"leadId": "L1", "resumen": "x"}, "cid-1")

    assert sobre["eventType"] == "ResumenGenerado"
    assert sobre["version"] == 1
    assert sobre["occurredAt"] == sobre["timestamp"]
    assert sobre["correlationId"] == "cid-1"
    assert sobre["payload"] == {"leadId": "L1", "resumen": "x"}
    assert "destino" not in sobre  # solo los comandos llevan destino


def test_los_topicos_son_los_tres_del_ecosistema() -> None:
    assert (kafka_settings.topic_comandos, kafka_settings.topic_eventos, kafka_settings.topic_dlt) == (
        "crm.comandos",
        "crm.eventos",
        "crm.dlt",
    )
