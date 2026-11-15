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

El servicio escucha el tópico `crm.comandos` y atiende el comando `GenerarResumen`
(con `destino: ia-resumen`) que le publica el orquestador cuando un lead queda
calificado; los comandos de otros servicios se ignoran. Contrato completo:
`docs/08-contrato-tres-topicos.md` del orquestador. Por cada comando:

1. Consulta `GET /clientes/{clienteId}` y `GET /vehiculos/{vehiculoId}` para
   obtener nombre/apellido y marca/modelo/anio.
2. Arma el `ClientePotencial` con solo los campos comerciales permitidos.
3. Genera el resumen con la IA (Ollama).
4. Publica el evento `ResumenGenerado` en `crm.eventos` con `{ leadId, resumen }` (en el sobre común, con el `correlationId` del comando).
5. Si el comando falla tras 3 intentos, lo manda tal cual a `crm.dlt`.

Variables adicionales:

| Variable | Descripción | Ejemplo |
|----------|-------------|---------|
| `CLIENTES_URL` | URL del microservicio de clientes | `http://localhost:8082` |
| `VEHICULOS_URL` | URL del microservicio de vehículos | `http://localhost:8083` |
| `ORQUESTADOR_CLAVE_SERVICIO` | Credencial de ia-resumen ante el orquestador (API Gateway). Obligatoria si `CLIENTES_URL` y `VEHICULOS_URL` apuntan al orquestador (`<orquestador>/api`): se manda en `X-Clave-Servicio`, junto con `X-Origen-Servicio: ia-resumen`. Vacía si se llama directo a los servicios | `(32+ caracteres)` |
| `KAFKA_ENABLED` | Activa/desactiva el consumidor Kafka | `true` |
| `KAFKA_BOOTSTRAP_SERVERS` | Servidores Kafka | `localhost:9092` |
| `KAFKA_TOPICO_COMANDOS` | Topic de entrada: las órdenes del orquestador. Atiende `GenerarResumen` (con `destino: ia-resumen`) e ignora el resto | `crm.comandos` |
| `KAFKA_TOPICO_EVENTOS` | Topic de salida: publica el evento `ResumenGenerado` | `crm.eventos` |
| `KAFKA_TOPICO_DLT` | Topic al que manda el comando que agotó sus reintentos | `crm.dlt` |
| `KAFKA_REINTENTOS_COMANDO` / `KAFKA_ESPERA_REINTENTO_SEGUNDOS` | Intentos antes de mandar a la DLT y espera inicial (se duplica) | `3` / `2` |
| `KAFKA_CONSUMER_GROUP` | Grupo consumidor | `resumen-ia-service` |
| `KAFKA_CLIENT_ID` | ID del cliente Kafka | `resumen-ia-microservice` |
| `KAFKA_SECURITY_PROTOCOL` | Protocolo de seguridad (Aiven: `SASL_SSL`) | `SASL_SSL` |
| `KAFKA_SASL_MECHANISM` | Mecanismo SASL (Aiven: `PLAIN`) | `PLAIN` |
| `KAFKA_SASL_USERNAME` / `KAFKA_SASL_PASSWORD` | Credenciales SASL del broker | `avnadmin` / `...` |
| `KAFKA_SSL_CAFILE` | Certificado CA (Aiven exige `ca.pem`) | `ca.pem` |

