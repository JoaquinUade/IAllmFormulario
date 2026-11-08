"""Punto de entrada de la API de resumen comercial de clientes."""

from contextlib import asynccontextmanager

from dotenv import load_dotenv

from fastapi import FastAPI

from app.kafka import start_consumer, stop_consumer, stop_producer
from app.routers import resumen

load_dotenv()


@asynccontextmanager
async def lifespan(app: FastAPI):
    await start_consumer()
    yield
    await stop_consumer()
    await stop_producer()


app = FastAPI(
    title="Resumen comercial de clientes",
    description="Genera un resumen breve de una oportunidad de compra de vehículo.",
    version="1.0.0",
    lifespan=lifespan,
)

app.include_router(resumen.router)
