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

Genera un resumen usando OpenAI. Para habilitarlo, configurar la clave antes
de levantar la API:

```powershell
$env:OPENAI_API_KEY = "tu-clave"
$env:OPENAI_MODEL = "gpt-4o-mini"
```

La API solo envía a la IA los campos comerciales definidos en los modelos
Pydantic; ignora CUIL, IDs y precio del vehículo. Si no se configura
`OPENAI_API_KEY`, `/resumen-ia` devuelve un error explícito y `/resumen`
continúa funcionando sin IA.

El servicio está preparado para Render mediante [`render.yaml`](./render.yaml).
