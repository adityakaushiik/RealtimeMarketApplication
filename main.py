import asyncio
from contextlib import asynccontextmanager
from sqlalchemy import select

from fastapi import FastAPI
from starlette.middleware.cors import CORSMiddleware

from config.database_config import close_database_engine, get_db_session
from config.logger import logger
from features.auth.auth_routes import auth_router
from features.instrument_type.instrument_type_routes import instrument_type_router
from features.instruments.instrument_routes import instrument_router
from features.exchange.exchange_routes import exchange_router
from features.populate_database.populate_database_router import populate_database_route
from features.provider.provider_routes import provider_router
from features.sector.sector_routes import sector_router
from features.websocket.web_socket_routes import websocket_route
from features.marketdata.marketdata_routes import marketdata_router  # added
from services.data.data_ingestion import LiveDataIngestion
from services.data.data_saver import DataSaver
from services.data.exchange_data import ExchangeData
from services.redis_subscriber import redis_subscriber
from models import Exchange

import sentry_sdk

sentry_sdk.init(
    dsn="https://87837fe7f05ab475836caf4864a1c150@o4510497758576640.ingest.us.sentry.io/4510497760673792",
    # Add data like request headers and IP for users,
    # see https://docs.sentry.io/platforms/python/data-management/data-collected/ for more info
    send_default_pii=True,
    # Enable sending logs to Sentry
    enable_logs=True,
    # Set traces_sample_rate to 1.0 to capture 100%
    # of transactions for tracing.
    traces_sample_rate=1.0,
    # Set profile_session_sample_rate to 1.0 to profile 100%
    # of profile sessions.
    profile_session_sample_rate=1.0,
    # Set profile_lifecycle to "trace" to automatically
    # run the profiler on when there is an active transaction
    profile_lifecycle="trace",
)

# Background task reference
subscriber_task = None
live_data_ingestion: LiveDataIngestion | None = None
data_saver: DataSaver | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage application lifecycle - startup and shutdown."""
    global subscriber_task, live_data_ingestion, data_broadcast, data_saver

    logger.info("Task - 1. Starting Live Data Ingestion...")
    live_data_ingestion = LiveDataIngestion()
    asyncio.create_task(live_data_ingestion.start_ingestion())

    # Start Subscriber
    logger.info("Task - 2. Starting Redis subscriber...")
    subscriber_task = asyncio.create_task(redis_subscriber())

    logger.info("Task - 3. Starting Data Saver...")
    data_saver = DataSaver()
    # Load exchanges and add to data_saver
    async for session in get_db_session():
        result = await session.execute(
            select(Exchange).where(Exchange.is_active == True)
        )
        exchanges = result.scalars().all()
        for exchange in exchanges:
            if (
                    exchange.market_open_time is not None
                    and exchange.market_close_time is not None
                    and exchange.timezone is not None
            ):
                exchange_data = ExchangeData(
                    exchange_name=exchange.name,
                    exchange_id=exchange.id,
                    market_open_time=exchange.market_open_time,
                    market_close_time=exchange.market_close_time,
                    pre_market_open_time=exchange.pre_market_open_time,
                    post_market_close_time=exchange.post_market_close_time,
                    timezone_str=exchange.timezone,
                )
                data_saver.add_exchange(exchange_data)
    await data_saver.start_all_exchanges(
        interval_minutes=5
    )  # Save every 5 minute for testing

    # async for session in get_db_session():
    #     data_creation_service = DataCreationService(session)
    #     Example usage (if you were running it manually)
    # await data_creation_service.start_data_creation(offset_days=0)

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
    allow_origins=["*"],  # Allows all origins
    allow_credentials=True,
    allow_methods=["*"],  # Allows all HTTP methods (GET, POST, etc.)
    allow_headers=["*"],  # Allows all headers
)

app.include_router(websocket_route)
app.include_router(auth_router)
app.include_router(instrument_router)
app.include_router(exchange_router)
app.include_router(provider_router)
app.include_router(marketdata_router)
app.include_router(populate_database_route)
app.include_router(sector_router)
app.include_router(instrument_type_router)
