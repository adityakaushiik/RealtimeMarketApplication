import asyncio
from contextlib import asynccontextmanager
from sqlalchemy import select

from fastapi import FastAPI
from starlette.middleware.cors import CORSMiddleware

from config.database_config import close_database_engine, get_db_session
from config.redis_config import close_redis_client
from config.logger import logger
from features.auth.auth_routes import auth_router
from features.instrument_type.instrument_type_routes import instrument_type_router
from features.instruments.instrument_routes import instrument_router
from features.exchange.exchange_routes import exchange_router
from features.populate_database.populate_database_router import populate_database_route
from features.provider.provider_routes import provider_router
from features.sector.sector_routes import sector_router
from features.user.user_routes import user_router
from features.websocket.web_socket_routes import websocket_route
from features.marketdata.marketdata_routes import marketdata_router  # added
from features.watchlist.watchlist_routes import watchlist_router
from features.suggestion.suggestion_router import router as suggestion_router
from features.health.health_routes import health_router
from services.data.data_ingestion import LiveDataIngestion, get_provider_manager
from services.data.data_saver import DataSaver
from services.data.data_resolver import DataResolver
from services.data.gap_detection import (
    get_gap_detection_service,
    set_gap_detection_provider,
)
from services.provider.provider_manager import ProviderManager
from services.data.redis_mapping import get_redis_mapping_helper
from services.redis_subscriber import redis_subscriber
from services.redis_timeseries import get_redis_timeseries
from services.scheduled_jobs import get_scheduled_jobs
from models import Exchange, Instrument, ProviderInstrumentMapping

import sentry_sdk

from utils.parse_file import parse_excel_file, parse_csv_file

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
provider_manager: ProviderManager | None = None
scheduled_jobs = None
gap_detection_service = None
data_broadcast = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage application lifecycle - startup and shutdown."""
    global subscriber_task, live_data_ingestion, data_broadcast, data_saver, provider_manager, scheduled_jobs, gap_detection_service

    # data = await parse_csv_file(file_path=r"C:\Users\tech\OneDrive\Documents\BSE_INSTRUMENTS_CSV1111.csv")
    #
    # async for session in get_db_session():
    #     session.expire_on_commit = False  # Prevent MissingGreenlet error
    #
    #     # Load existing instruments and mappings once
    #     instruments = await session.execute(
    #         select(Instrument).where(
    #             Instrument.exchange_id == 8,
    #             Instrument.instrument_type_id == 1
    #         )
    #     )
    #     instruments = instruments.scalars().all()
    #
    #     mappings = await session.execute(
    #         select(ProviderInstrumentMapping).where(
    #             ProviderInstrumentMapping.provider_id == 2
    #         )
    #     )
    #     mappings = mappings.scalars().all()
    #
    #     # Make quick lookup: symbol → instrument
    #     symbol_to_inst = {inst.symbol.replace(".BSE", ""): inst for inst in instruments}
    #
    #     # Make quick lookup: instrument_id → mapping
    #     inst_id_to_mapping = {m.instrument_id: m for m in mappings}
    #
    #     updated_count = 0
    #
    #     for row in data:
    #         symbol = row.get("SEM_TRADING_SYMBOL")
    #         new_search_code = row.get("SEM_SMST_SECURITY_ID")
    #
    #         if not symbol or not new_search_code:
    #             continue
    #
    #         # Find instrument by symbol
    #         inst = symbol_to_inst.get(symbol)
    #         if not inst:
    #             print(f"Skipping - no instrument found for: {symbol}")
    #             continue
    #
    #         # Find existing mapping
    #         mapping = inst_id_to_mapping.get(inst.id)
    #         if not mapping:
    #             print(f"No mapping exists for {symbol} (id={inst.id}) → skipping")
    #             continue
    #
    #         # Update only if different
    #         if mapping.provider_instrument_search_code != new_search_code:
    #             mapping.provider_instrument_search_code = new_search_code
    #             updated_count += 1
    #             print(f"Updated {symbol} → {new_search_code}")
    #
    #     # One single commit at the end
    #     if updated_count > 0:
    #         await session.commit()
    #         print(f"\nSuccessfully updated {updated_count} mappings.")
    #     else:
    #         print("\nNo updates needed - all search codes already match.")
    #
    # return

    # --- Startup ---
    logger.info("Starting up application...")

    # Sync Redis Mappings
    try:
        await get_redis_mapping_helper().sync_mappings_from_db()
    except Exception as e:
        logger.error(f"Failed to sync Redis mappings on startup: {e}")

    logger.info("Task - 1. Starting Live Data Ingestion...")
    live_data_ingestion = LiveDataIngestion()

    # Create Provider Manager and set callback
    provider_manager = get_provider_manager()

    # Task - 1.5: Initialize Redis TimeSeries downsampling for recordable instruments
    logger.info(
        "Task - 1.5: Initializing Redis TimeSeries for recordable instruments..."
    )
    try:
        redis_ts = get_redis_timeseries()
        # Fetch recordable symbols from Redis mappings (already synced above)
        mapper = get_redis_mapping_helper()
        record_map = await mapper.get_symbol_record_map()

        recordable_symbols = [
            symbol for symbol, should_record in record_map.items() if should_record
        ]

        if recordable_symbols:
            await redis_ts.initialize_recordable_instruments(recordable_symbols)
    except Exception as e:
        logger.error(f"Failed to initialize Redis TimeSeries: {e}")

    await provider_manager.initialize()  # Ensure mappings are loaded before use

    asyncio.create_task(live_data_ingestion.start_ingestion())

    # # Task - 1.6: Initialize Gap Detection Service and fill gaps from startup
    logger.info("Task - 1.6: Initializing Gap Detection Service...")
    try:
        gap_detection_service = get_gap_detection_service()
        set_gap_detection_provider(provider_manager)
        await gap_detection_service.initialize()
        # Run gap detection and filling in background
        asyncio.create_task(gap_detection_service.fill_gaps_from_startup())
    except Exception as e:
        logger.error(f"Failed to initialize gap detection: {e}")

    # Task - 1.1. Checking for data gaps...
    # Moved to scheduled_jobs to prevent race condition/double logging at startup
    data_resolver = DataResolver(provider_manager)
    # asyncio.create_task(data_resolver.check_and_fill_gaps())

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
                # Add exchange directly to data_saver
                data_saver.add_exchange(exchange)

    # Also start end-of-day monitors for final data consolidation
    await data_saver.start_end_of_day_monitors()

    # # Task - 4. Start Scheduled Jobs (A4: periodic gap detection, A2: retry alerts, B3: volume reconciliation)
    logger.info("Task - 4. Starting Scheduled Jobs...")
    scheduled_jobs = get_scheduled_jobs()
    scheduled_jobs.set_resolver(data_resolver)
    await scheduled_jobs.start()

    yield

    logger.info("Stopping background tasks...")

    # Stop scheduled jobs
    if scheduled_jobs:
        await scheduled_jobs.stop()

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
    # if data_broadcast and data_broadcast.broadcast_task:
    #     data_broadcast.broadcast_task.cancel()
    #     try:
    #         await data_broadcast.broadcast_task
    #     except asyncio.CancelledError:
    #         pass

    if live_data_ingestion:
        live_data_ingestion.stop_ingestion()

    # Close Redis client
    await close_redis_client()

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
app.include_router(watchlist_router)
app.include_router(populate_database_route)
app.include_router(sector_router)
app.include_router(instrument_type_router)
app.include_router(health_router)
app.include_router(user_router)
app.include_router(suggestion_router)
