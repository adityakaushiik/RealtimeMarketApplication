import asyncio
from contextlib import asynccontextmanager
from typing import List, Dict

from sqlalchemy import select, update, func

from fastapi import FastAPI
from sqlalchemy.ext.asyncio import AsyncSession
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
from services.data.data_ingestion import LiveDataIngestion
from services.data.data_saver import DataSaver
from services.data.data_resolver import DataResolver
from services.data.gap_detection import (
    get_gap_detection_service,
    set_gap_detection_provider,
)
from services.provider.provider_manager import ProviderManager, get_provider_manager
from services.data.redis_mapping import get_redis_mapping_helper
from services.redis_subscriber import redis_subscriber
from services.redis_timeseries import get_redis_timeseries
from services.scheduled_jobs import get_scheduled_jobs
from models import Exchange, Instrument, ProviderInstrumentMapping

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
provider_manager: ProviderManager | None = None
scheduled_jobs = None
gap_detection_service = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage application lifecycle - startup and shutdown."""
    global subscriber_task, live_data_ingestion, data_saver, provider_manager, scheduled_jobs, gap_detection_service

    # data = await parse_csv_file(file_path=r"C:\Users\tech\OneDrive\Documents\BSE_INSTRUMENTS_CSV1111.csv")
    #
    # async for session in get_db_session():
    #     # 1. Get existing symbols once (fast)
    #     existing = await session.execute(
    #         select(Instrument.symbol).where(
    #             Instrument.exchange_id == 8,
    #             Instrument.instrument_type_id == 1
    #         )
    #     )
    #     existing_symbols = {row[0].replace(".BSE", "") for row in existing}
    #
    #     # 2. Create missing ones
    #     await create_new_instruments_and_mappings(session, data, existing_symbols)
    #
    #     # 3. Update search codes for existing
    #     updated = await update_existing_search_codes(session, data)
    #     print(f"Updated {updated} search codes")
    #
    # return

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

    # Start periodic saving for active exchanges
    # Changed interval to 60 minutes (Hourly) as requested ("Buffer Zone" Strategy)
    # This ensures that even if the session crashes, we have data persisted every hour.
    await data_saver.start_all_exchanges(interval_minutes=60)

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


async def create_new_instruments_and_mappings(
    session: AsyncSession,
    csv_data: List[Dict],
    existing_symbols: set[str],
    batch_size: int = 300,
) -> None:
    """
    Creates ONLY new instruments + their mappings.
    - Skips anything that already exists in DB
    - Uses batching + flush + single commit per batch for speed
    """
    new_instruments = []
    new_mappings = []

    for row in csv_data:
        symbol = row.get("UNDERLYING_SYMBOL")
        if not symbol or symbol in existing_symbols:
            continue

        instrument = Instrument(
            exchange_id=8,
            instrument_type_id=1,
            symbol=f"{symbol}.BSE",
            name=row.get("DISPLAY_NAME"),
            is_active=True,
            should_record_data=False,
        )
        new_instruments.append((instrument, row))

        if len(new_instruments) >= batch_size:
            await _process_creation_batch(session, new_instruments, new_mappings)
            new_instruments.clear()
            new_mappings.clear()

    # Last batch
    if new_instruments:
        await _process_creation_batch(session, new_instruments, new_mappings)


async def _process_creation_batch(
    session: AsyncSession,
    instruments_with_row: List[tuple[Instrument, dict]],
    mappings_list: List[ProviderInstrumentMapping],
) -> None:
    # Add all instruments
    for instrument, _ in instruments_with_row:
        session.add(instrument)

    await session.flush()  # Get all IDs in one go

    # Create mappings
    for instrument, row in instruments_with_row:
        mapping = ProviderInstrumentMapping(
            provider_id=2,
            instrument_id=instrument.id,
            provider_instrument_search_code=row.get("SECURITY_ID"),
            is_active=True,
            provider_instrument_segment="BSE_EQ",
        )
        mappings_list.append(mapping)
        session.add(mapping)

    await session.commit()
    logger.info(
        f"Created batch of {len(instruments_with_row)} new instruments + mappings"
    )


# ──────────────────────────────────────────────────────────────────────────────


async def update_existing_search_codes(
    session: AsyncSession, csv_data: List[Dict], batch_size: int = 500
) -> int:
    """
    Updates ONLY provider_instrument_search_code for EXISTING instruments.
    - Very fast: bulk update style + minimal loading
    - Returns number of updated rows
    """
    session.expire_on_commit = False

    # Get minimal data - only what we need
    result = await session.execute(
        select(Instrument.id, Instrument.symbol).where(
            Instrument.exchange_id == 8, Instrument.instrument_type_id == 1
        )
    )
    inst_map = {row.symbol.replace(".BSE", ""): row.id for row in result}

    if not inst_map:
        logger.info("No BSE equity instruments found in database")
        return 0

    updates_count = 0
    update_statements = []

    for row in csv_data:
        symbol = row.get("UNDERLYING_SYMBOL")
        new_code = row.get("SECURITY_ID")

        if not symbol or not new_code:
            continue

        instrument_id = inst_map.get(symbol)
        if not instrument_id:
            continue

        # We'll collect bulk update statements
        update_statements.append(
            update(ProviderInstrumentMapping)
            .where(
                ProviderInstrumentMapping.provider_id == 2,
                ProviderInstrumentMapping.instrument_id == instrument_id,
                ProviderInstrumentMapping.provider_instrument_search_code != new_code,
            )
            .values(provider_instrument_search_code=new_code)
            .execution_options(synchronize_session=False)
        )

        updates_count += 1  # optimistic count

        if len(update_statements) >= batch_size:
            for stmt in update_statements:
                await session.execute(stmt)
            await session.commit()
            update_statements.clear()
            logger.info(f"Processed update batch ({updates_count} potential updates)")

    # Final batch
    if update_statements:
        for stmt in update_statements:
            await session.execute(stmt)
        await session.commit()

    actual_updated = await _get_actual_updated_count(session)  # optional verification
    logger.info(
        f"Update completed. Potential: {updates_count}, actual changes may be less."
    )
    return actual_updated or updates_count


async def _get_actual_updated_count(session: AsyncSession) -> int:
    """Optional: count how many were actually different"""
    result = await session.execute(
        select(func.count())
        .select_from(ProviderInstrumentMapping)
        .where(ProviderInstrumentMapping.provider_id == 2)
        # You could add more precise tracking if needed
    )
    return result.scalar() or 0
