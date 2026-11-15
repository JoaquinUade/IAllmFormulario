"""ia-resumen se identifica ante el orquestador solo si tiene su credencial configurada."""

from app.kafka import config


def test_sin_clave_no_manda_headers(monkeypatch):
    monkeypatch.setattr(config, "ORQUESTADOR_CLAVE_SERVICIO", None)
    assert config.headers_de_servicio() == {}


def test_con_clave_se_identifica_como_ia_resumen(monkeypatch):
    monkeypatch.setattr(config, "ORQUESTADOR_CLAVE_SERVICIO", "x" * 32)
    assert config.headers_de_servicio() == {
        "X-Origen-Servicio": "ia-resumen",
        "X-Clave-Servicio": "x" * 32,
    }
