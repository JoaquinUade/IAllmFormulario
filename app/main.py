"""Punto de entrada de la API de resumen comercial de clientes."""

from dotenv import load_dotenv

from fastapi import FastAPI

from app.routers import resumen

load_dotenv()

app = FastAPI(
    title="Resumen comercial de clientes",
    description="Genera un resumen breve de una oportunidad de compra de vehículo.",
    version="1.0.0",
)

app.include_router(resumen.router)
