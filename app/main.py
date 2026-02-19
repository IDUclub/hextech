import sys
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, RedirectResponse
from iduconfig import Config
from loguru import logger
from otteroad import KafkaConsumerService, KafkaConsumerSettings

from app.common.broker.broker_service import BrokerService
from app.common.middlewares.exception_handler import ExceptionHandlerMiddleware

from .__version__ import APP_VERSION
from .common.middlewares.prometheus_handler import ObservabilityMiddleware
from .grid_generator import grid_generator_router
from .indicators_savior import indicators_savior_router
from .limitations import limitations_router
from .observability import OpenTelemetryAgent, PrometheusConfig
from .observability.metrics import setup_metrics
from .prioc import prioc_router

logger.remove()
logger.add(
    sys.stdout,
    format="<green>{time:MM-DD HH:mm}</green> | <level>{level:<8}</level> | <cyan>{message}</cyan>",
    level="INFO",
    colorize=True,
)
logger.add(".log", colorize=False, backtrace=True, diagnose=True)

config = Config()
consumer_settings = KafkaConsumerSettings.from_env()

broker_client = KafkaConsumerService(consumer_settings)
broker_service = BrokerService(config, broker_client)
metrics = setup_metrics()


@asynccontextmanager
async def lifespan(app: FastAPI):
    otel_agent = OpenTelemetryAgent(
        prometheus_config=PrometheusConfig(
            host="0.0.0.0",
            port=int(config.get("PROMETHEUS_PORT")),
        ),
    )
    await broker_service.register_and_start()
    yield
    await broker_service.stop()
    otel_agent.shutdown()


app = FastAPI(
    lifespan=lifespan,
    version=APP_VERSION,
    title="hextech",
    description="API for spatial hexagonal analyses",
)

origins = ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.add_middleware(ExceptionHandlerMiddleware, metrics=metrics)
app.add_middleware(ObservabilityMiddleware, metrics=metrics)


@app.get("/logs")
async def get_logs():
    """
    Get app logs
    """

    return FileResponse(
        ".log",
        media_type="application/octet-stream",
        filename=f"Hextech.log",
    )


app.include_router(prioc_router, prefix=config.get("FASTAPI_PREFIX"))
app.include_router(grid_generator_router, prefix=config.get("FASTAPI_PREFIX"))
app.include_router(limitations_router, prefix=config.get("FASTAPI_PREFIX"))
app.include_router(indicators_savior_router, prefix=config.get("FASTAPI_PREFIX"))


@app.get("/", include_in_schema=False)
async def docs_redirect():
    return RedirectResponse(url="/docs")
