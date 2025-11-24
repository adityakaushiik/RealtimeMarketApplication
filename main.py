import asyncio
from contextlib import asynccontextmanager
from sqlalchemy import select

from fastapi import FastAPI
from starlette.middleware.cors import CORSMiddleware

from config.database_config import close_database_engine, get_db_session
from config.logger import logger
from features.auth.auth_routes import auth_router
from features.instruments.instrument_routes import instrument_router
from features.exchange.exchange_routes import exchange_router
from features.provider.provider_routes import provider_router
from features.websocket.web_socket_routes import websocket_route
from features.marketdata.marketdata_routes import marketdata_router  # added
from services.data.data_broadcast import DataBroadcast
from services.data.data_ingestion import LiveDataIngestion
from services.data.data_saver import DataSaver
from services.data.exchange_data import ExchangeData
from services.redis_subscriber import redis_subscriber
from models import Exchange

# Background task reference
subscriber_task = None
live_data_ingestion: LiveDataIngestion | None = None
data_broadcast: DataBroadcast | None = None
data_saver: DataSaver | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage application lifecycle - startup and shutdown."""
    global subscriber_task, live_data_ingestion, data_broadcast, data_saver

    logger.info("Task - 1. Starting Live Data Ingestion...")
    live_data_ingestion = LiveDataIngestion()
    asyncio.create_task(live_data_ingestion.start_ingestion())

    logger.info("Task - 2. Starting Data Broadcast...")
    data_broadcast = DataBroadcast()
    await data_broadcast.start_broadcast()

    # Start Subscriber
    logger.info("Task - 3. Starting Redis subscriber...")
    subscriber_task = asyncio.create_task(redis_subscriber())

    logger.info("Task - 4. Starting Data Saver...")
    data_saver = DataSaver()
    # Load exchanges and add to data_saver
    async for session in get_db_session():
        result = await session.execute(select(Exchange))
        exchanges = result.scalars().all()
        for exchange in exchanges:
            if (exchange.market_open_time is not None and
                exchange.market_close_time is not None and
                exchange.timezone is not None):
                exchange_data = ExchangeData(
                    exchange_name=exchange.name,
                    exchange_id=exchange.id,
                    market_open_time_hhmm=exchange.market_open_time,
                    market_close_time_hhmm=exchange.market_close_time,
                    timezone_str=exchange.timezone,
                )
                data_saver.add_exchange(exchange_data)
    await data_saver.start_all_exchanges(interval_minutes=1)  # Save every 1 minute for testing

    yield

    logger.info("Stopping background tasks...")
    # Stop data_saver
    if data_saver:
        await data_saver.stop_all_exchanges()

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
    allow_origins=["*"], # Allows all origins
    allow_credentials=True,
    allow_methods=["*"],  # Allows all HTTP methods (GET, POST, etc.)
    allow_headers=["*"],  # Allows all headers
)

app.include_router(websocket_route)
app.include_router(auth_router)
app.include_router(instrument_router)
app.include_router(exchange_router)
app.include_router(provider_router)
app.include_router(marketdata_router)  # added
