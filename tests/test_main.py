from fastapi.testclient import TestClient

from app.main import app
from app.services import ia


client = TestClient(app)


def test_genera_resumen_comercial_sin_exponer_datos_sensibles() -> None:
    payload = {
        "cliente": {"nombre": "Martina", "apellido": "López", "cuil": "20-12345678-9"},
        "vehiculo": {
            "id": "uuid-del-vehiculo",
            "marca": "Toyota",
            "modelo": "Corolla",
            "anio": 2022,
            "precio": 25000000,
        },
        "anticipo": 3000000,
        "permuta": {
            "tienePermuta": True,
            "descripcion": "Fiat Cronos 2019",
            "valorDeclarado": 3000000,
        },
        "ubicacion": {"provincia": "BUENOS_AIRES", "localidad": "San Isidro"},
        "solicitaFinanciacion": True,
    }

    response = client.post("/resumen", json=payload)

    assert response.status_code == 200
    resumen = response.json()["resumen"]
    assert resumen == (
        "Cliente: Martina López.\n"
        "Vehículo seleccionado: Toyota Corolla (2022).\n"
        "Ubicación: San Isidro, BUENOS AIRES. Anticipo: $3.000.000.\n"
        "Permuta: Sí, Fiat Cronos 2019, valor declarado $3.000.000. "
        "Financiación: Sí.\n"
        "Conclusión comercial: El asesor debería validar la tasación de la "
        "permuta y revisar las condiciones de financiación solicitadas."
    )
    assert "20-12345678-9" not in resumen
    assert len(resumen.splitlines()) == 5


def test_rechaza_importes_negativos() -> None:
    response = client.post(
        "/resumen",
        json={
            "cliente": {"nombre": "Ana", "apellido": "Pérez"},
            "vehiculo": {"marca": "Ford", "modelo": "Ka", "anio": 2020},
            "anticipo": -1,
        },
    )

    assert response.status_code == 422


def test_resumen_ia_usa_solo_datos_permitidos(monkeypatch) -> None:
    datos_enviados = {}

    class Respuesta:
        output_text = "Resumen generado por IA.\nSiguiente recomendación."

    class Respuestas:
        def create(self, **kwargs):
            datos_enviados.update(kwargs)
            return Respuesta()

    class ClienteIA:
        def __init__(self, **kwargs):
            self.responses = Respuestas()

    monkeypatch.setenv("OLLAMA_API_KEY", "test-key")
    monkeypatch.setattr(ia, "datos_para_ia", lambda datos: datos.model_dump_json())
    monkeypatch.setitem(__import__("sys").modules, "openai", type("OpenAI", (), {"OpenAI": ClienteIA}))

    response = client.post(
        "/resumen-ia",
        json={
            "cliente": {
                "nombre": "Martina",
                "apellido": "López",
                "cuil": "20-12345678-9",
            },
            "vehiculo": {
                "marca": "Toyota",
                "modelo": "Corolla",
                "anio": 2022,
                "id": "interno",
                "precio": 25000000,
            },
        },
    )

    assert response.status_code == 200
    assert response.json()["resumen"] == (
        "Resumen generado por IA.\nSiguiente recomendación."
    )
    assert "20-12345678-9" not in datos_enviados["input"]
    assert "interno" not in datos_enviados["input"]
    assert "25000000" not in datos_enviados["input"]


def test_resumen_ia_requiere_clave(monkeypatch) -> None:
    monkeypatch.delenv("OLLAMA_API_KEY", raising=False)

    response = client.post(
        "/resumen-ia",
        json={
            "cliente": {"nombre": "Ana", "apellido": "Pérez"},
            "vehiculo": {"marca": "Ford", "modelo": "Ka", "anio": 2020},
        },
    )

    assert response.status_code == 500
    assert response.json()["detail"] == (
        "La variable OLLAMA_API_KEY no está configurada."
    )
