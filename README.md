# API de resumen comercial

API individual en FastAPI para generar un resumen comercial de un potencial
cliente interesado en comprar un vehículo. La respuesta tiene como máximo
cinco líneas y no incluye campos extra del formulario, como CUIL, identificador
del vehículo o precio.

## Ejecución local

```bash
pip install -r requirements.txt
uvicorn app.main:app --reload
```

La documentación interactiva queda disponible en `http://localhost:8000/docs`.

## Endpoints

`POST /resumen`

Recibe el JSON del formulario y devuelve:

```json
{
  "resumen": "Cliente: ...\nVehículo seleccionado: ..."
}
```

`POST /resumen-ia`

Genera un resumen usando Ollama (compatible con la API de OpenAI). Para
habilitarlo, configurar las variables antes de levantar la API:

```powershell
$env:OLLAMA_API_KEY = "tu-clave-de-ollama"
$env:OLLAMA_MODEL = "nemotron-3-super:cloud"
```

Por defecto la API se conecta a Ollama Cloud (`https://ollama.com/v1`). Para
usar un servidor local, configurar también:

```powershell
$env:OLLAMA_BASE_URL = "http://localhost:11434/v1"
```

La API solo envía a la IA los campos comerciales definidos en los modelos
Pydantic; ignora CUIL, IDs y precio del vehículo. Si no se configura
`OLLAMA_API_KEY`, `/resumen-ia` devuelve un error explícito y `/resumen`
continúa funcionando sin IA.

El servicio está preparado para Render mediante [`render.yaml`](./render.yaml).

## Integración con Kafka

El servicio consume el tópico `resumen-solicitado` (publicado por el
microservicio de leads cuando un lead queda calificado). Por cada mensaje:

1. Consulta `GET /clientes/{clienteId}` y `GET /vehiculos/{vehiculoId}` para
   obtener nombre/apellido y marca/modelo/anio.
2. Arma el `ClientePotencial` con solo los campos comerciales permitidos.
3. Genera el resumen con la IA (Ollama).
4. Publica el resultado en `resumen-generado` con `{ leadId, resumen }`.

Variables adicionales:

| Variable | Descripción | Ejemplo |
|----------|-------------|---------|
| `CLIENTES_URL` | URL del microservicio de clientes | `http://localhost:8082` |
| `VEHICULOS_URL` | URL del microservicio de vehículos | `http://localhost:8083` |
| `KAFKA_ENABLED` | Activa/desactiva el consumidor Kafka | `true` |
| `KAFKA_BOOTSTRAP_SERVERS` | Servidores Kafka | `localhost:9092` |
| `KAFKA_TOPIC_RESUMEN_SOLICITADO` | Topic de entrada | `resumen-solicitado` |
| `KAFKA_TOPIC_RESUMEN_GENERADO` | Topic de salida | `resumen-generado` |
| `KAFKA_CONSUMER_GROUP` | Grupo consumidor | `resumen-ia-service` |
| `KAFKA_CLIENT_ID` | ID del cliente Kafka | `resumen-ia-microservice` |

