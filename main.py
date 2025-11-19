import asyncio
from contextlib import asynccontextmanager

from fastapi import FastAPI
from starlette.middleware.cors import CORSMiddleware

from config.database_config import close_database_engine
from config.logger import logger
from features.auth.auth_routes import auth_router
from features.instruments.instrument_routes import instrument_router
from features.exchange.exchange_routes import exchange_router
from features.provider.provider_routes import provider_router
from features.websocket.web_socket_routes import websocket_route
from services.data_broadcast import DataBroadcast
from services.live_data_ingestion import LiveDataIngestion
from services.redis_subscriber import redis_subscriber

# Background task reference
subscriber_task = None
live_data_ingestion: LiveDataIngestion | None = None
data_broadcast: DataBroadcast | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage application lifecycle - startup and shutdown."""
    global subscriber_task, live_data_ingestion, data_broadcast

    # Initialize RedisHelper and ensure prices_dict exists
    # redis_helper = RedisHelper()
    # await redis_helper.initialize_prices_dict()

    live_data_ingestion = LiveDataIngestion()
    # asyncio.create_task(live_data_ingestion.start_ingestion())

    data_broadcast = DataBroadcast()
    # await data_broadcast.start_broadcast()

    # Startup
    logger.info("🚀 Starting Redis subscriber...")
    subscriber_task = asyncio.create_task(redis_subscriber())

    yield

    logger.info("Stopping background tasks...")
    # Stop subscriber
    if subscriber_task:
        subscriber_task.cancel()
        try:
            await subscriber_task
        except asyncio.CancelledError:
            pass

    # Stop publisher
    if data_broadcast and data_broadcast.broadcast_task:
        data_broadcast.broadcast_task.cancel()
        try:
            await data_broadcast.broadcast_task
        except asyncio.CancelledError:
            pass

    if live_data_ingestion:
        live_data_ingestion.stop_ingestion()

    # Dispose DB engine so async connections close cleanly
    await close_database_engine()
    logger.info("Shutdown complete")


app = FastAPI(
    title="Realtime Application API",
    description="API documentation for the Realtime Application",
    version="1.0.0",
    lifespan=lifespan,
    swagger_url="/",
)

# Add CORS middleware correctly
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:4200"],
    allow_credentials=True,
    allow_methods=["*"],  # Allows all HTTP methods (GET, POST, etc.)
    allow_headers=["*"],  # Allows all headers
)

app.include_router(websocket_route)
app.include_router(auth_router)
app.include_router(instrument_router)
app.include_router(exchange_router)
app.include_router(provider_router)
