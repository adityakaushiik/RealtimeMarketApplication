from collections import defaultdict
from datetime import datetime, timezone, timedelta
from typing import Type, List, Dict, Tuple, Any
import pytz

from sqlalchemy import select, update, text, func
from sqlalchemy.orm import joinedload

from config.database_config import get_db_session
from config.logger import logger
from config.redis_config import get_redis
from models import Instrument, PriceHistoryIntraday, PriceHistoryDaily
from services.provider.provider_manager import ProviderManager

class DataResolver:
    """
    Resolves missing or incomplete price data by fetching from providers.
    Target records are those with resolve_required=True.
    """

    def __init__(self, provider_manager: ProviderManager):
        self.provider_manager = provider_manager

    async def resolve_all(self):
        """Resolve both intraday and daily prices."""
        await self.resolve_intraday_prices()
        await self.resolve_daily_prices()

    async def check_and_fill_gaps(self):
        """
        Checks for gaps in data updates since the last save and marks existing records
        for resolution.
        """

        redis = get_redis()
        if not redis:
            logger.warning("Redis not available for gap check")
            return

        try:
            last_save_ts_str = await redis.get("last_save_time:5m")
            if not last_save_ts_str:
                logger.info("No last_save_time found in Redis. Skipping gap check.")
                # Even if no gap check, we should try to resolve any pending records
                await self.resolve_intraday_prices()
                return

            last_save_ts = int(last_save_ts_str)
            last_save_dt = datetime.fromtimestamp(last_save_ts / 1000, tz=timezone.utc)
            
            now = datetime.now(timezone.utc)
            
            if last_save_dt >= now:
                logger.info("Last save time is in the future or current. No gaps to check.")
                # Even if no gaps, resolve pending
                await self.resolve_intraday_prices()
                return

            logger.info(f"Checking for missed updates between {last_save_dt} and {now}")
            
            async for session in get_db_session():
                # Get active instruments that should have data recorded
                stmt_instruments = select(Instrument.id).where(Instrument.should_record_data == True)
                result = await session.execute(stmt_instruments)
                instrument_ids = result.scalars().all()
                
                if not instrument_ids:
                    logger.info("No instruments marked for data recording.")
                    return

                # Update existing records in this time range to resolve_required=True
                # We assume records were pre-created by DataCreationService
                # Also increment resolve_tries
                stmt = (
                    update(PriceHistoryIntraday)
                    .where(
                        PriceHistoryIntraday.datetime > last_save_dt,
                        PriceHistoryIntraday.datetime < now,
                        PriceHistoryIntraday.instrument_id.in_(instrument_ids)
                    )
                    .values(
                        resolve_required=True,
                        resolve_tries=PriceHistoryIntraday.resolve_tries + 1
                    )
                )
                
                result = await session.execute(stmt)
                updated_count = result.rowcount
                await session.commit()
                
                if updated_count > 0:
                    logger.info(f"Marked {updated_count} records for resolution between {last_save_dt} and {now}")
                else:
                    logger.info("No records found to mark for resolution.")
            
            # Trigger resolution for the newly inserted records (and any old ones)
            await self.resolve_intraday_prices()

        except Exception as e:
            logger.error(f"Error in check_and_fill_gaps: {e}", exc_info=True)

    async def resolve_intraday_prices(self):
        """Resolve intraday prices marked as requiring resolution."""
        await self._resolve_prices(PriceHistoryIntraday, "get_intraday_prices")

    async def resolve_daily_prices(self):
        """
        Resolve daily prices marked as requiring resolution.
        For today's records, we aggregate from intraday data because some providers
        (like Dhan) don't provide daily candles for the current day.
        """
        # 1. Resolve historical daily prices (yesterday and before) normally
        # We can filter for records strictly before today if needed, but _resolve_prices
        # will just try to fetch whatever is marked.
        # However, to be efficient and handle the "today" case specifically, we should:

        # A. Resolve standard daily prices (likely historical)
        await self._resolve_prices(PriceHistoryDaily, "get_daily_prices")

        # B. Aggregate intraday for today's daily record
        await self._aggregate_intraday_to_daily_today()

    async def _aggregate_intraday_to_daily_today(self):
        """
        Aggregates resolved intraday prices for the current day to update/resolve
        the daily price record for today.

        Updated to be timezone-aware:
        Groups instruments by exchange timezone to correctly determine the "start of day"
        in UTC for each instrument.
        """
        logger.info("🔍 Starting aggregation of intraday prices to daily for today")

        # We can't just use a single "today_start" in UTC because "today" starts at different
        # UTC times for different exchanges.
        # We need to fetch daily records that need resolution, then group them by exchange timezone.

        async for session in get_db_session():
            try:
                # 1. Find daily records for "today" (or recent) that need resolution
                # We filter loosely by UTC time first to reduce the set, then refine.
                # Let's say anything in the last 24-48 hours.
                # Actually, we should look for records where resolve_required is True.
                # And we assume these records were created with the correct "date" (midnight UTC usually for daily records).

                # However, the user says: "if the data is in utc and was created for utc date for the day 20-12-2025 then it would lie into 2 dates for NASDAQ"
                # This implies the daily record's `datetime` column might be 2025-12-20 00:00:00 UTC.
                # But for NASDAQ, the trading day 2025-12-20 is from 14:30 UTC to 21:00 UTC.

                # So if we have a daily record for 2025-12-20, we want to aggregate intraday data
                # that falls within that exchange's trading day.

                # Let's fetch the daily records needing resolution.
                today_start = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
                today_end = today_start + timedelta(days=1)

                stmt = (
                    select(PriceHistoryDaily)
                    .options(joinedload(PriceHistoryDaily.instrument).joinedload(Instrument.exchange))
                    .where(
                        PriceHistoryDaily.resolve_required == True,
                        # Only aggregate for TODAY. Historical data should be fetched from provider.
                        PriceHistoryDaily.datetime >= today_start,
                        PriceHistoryDaily.datetime < today_end
                    )
                )
                result = await session.execute(stmt)
                daily_records = result.scalars().all()

                if not daily_records:
                    logger.info("🔍 No daily records for today needing resolution via aggregation.")
                    return

                logger.info(f"🔍 Found {len(daily_records)} daily records to aggregate from intraday.")

                # Group by (Exchange, Date)
                # We need to know the "Date" of the daily record to find the start/end in UTC.
                # Assuming daily_record.datetime is midnight UTC of the date.

                grouped_records = defaultdict(list)

                for record in daily_records:
                    instrument = record.instrument
                    exchange = instrument.exchange

                    # Key: (exchange_id, date_date_object)
                    # record.datetime is a datetime object. We take the date part.
                    target_date = record.datetime.date()
                    grouped_records[(exchange, target_date)].append(record)

                # Process each group
                total_updated = 0

                for (exchange, target_date), records in grouped_records.items():
                    tz_name = exchange.timezone or "UTC"
                    try:
                        tz = pytz.timezone(tz_name)
                    except pytz.UnknownTimeZoneError:
                        logger.warning(f"🔍 Unknown timezone {tz_name}, defaulting to UTC")
                        tz = pytz.UTC

                    # Calculate start of day using market open time
                    # If market times are missing, fallback to full day? Or skip?
                    # Let's fallback to full day if missing, but they should be there.
                    open_time = exchange.market_open_time or datetime.min.time()
                    close_time = exchange.market_close_time or datetime.max.time() # Actually max time is 23:59:59...

                    # Create a naive datetime for market open on that date
                    naive_start = datetime.combine(target_date, open_time)

                    # Localize it to the exchange timezone
                    try:
                        local_start = tz.localize(naive_start)
                    except Exception:
                        local_start = naive_start.replace(tzinfo=tz)

                    # Convert to UTC
                    utc_start = local_start.astimezone(timezone.utc)

                    # End of day (market close)
                    naive_end = datetime.combine(target_date, close_time)

                    # Handle case where close time is earlier than open time (crossing midnight)
                    # Though for daily aggregation we usually assume trading day is contiguous.
                    # If close < open, it might mean next day.
                    if close_time < open_time:
                         naive_end += timedelta(days=1)

                    try:
                        local_end = tz.localize(naive_end)
                    except Exception:
                        local_end = naive_end.replace(tzinfo=tz)
                    utc_end = local_end.astimezone(timezone.utc)

                    logger.info(
                        f"🔍 Aggregating batch: Exchange={exchange.name}, Date={target_date}. "
                        f"UTC Window: {utc_start} to {utc_end}. Instruments: {len(records)}"
                    )

                    # Collect instrument IDs for this batch
                    instrument_ids = [r.instrument_id for r in records]

                    # Map instrument_id to record for update
                    record_map = {r.instrument_id: r for r in records}

                    # Query intraday data
                    # We want data >= utc_start AND < utc_end

                    query = text("""
                        SELECT 
                            instrument_id,
                            first(open, datetime) as open,
                            max(high) as high,
                            min(low) as low,
                            last(close, datetime) as close,
                            sum(volume) as volume
                        FROM price_history_intraday
                        WHERE instrument_id = ANY(:instrument_ids)
                          AND datetime >= :start_time
                          AND datetime < :end_time
                        GROUP BY instrument_id
                    """)

                    result = await session.execute(query, {
                        "instrument_ids": instrument_ids,
                        "start_time": utc_start,
                        "end_time": utc_end
                    })

                    rows = result.fetchall()

                    for row in rows:
                        inst_id = row.instrument_id
                        if inst_id in record_map:
                            record = record_map[inst_id]
                            record.open = row.open
                            record.high = row.high
                            record.low = row.low
                            record.close = row.close
                            record.volume = row.volume
                            record.resolve_required = False
                            session.add(record)
                            total_updated += 1

                await session.commit()
                logger.info(f"🔍 Successfully aggregated {total_updated} daily records from intraday data.")

            except Exception as e:
                logger.error(f"🔍 Error aggregating intraday to daily: {e}", exc_info=True)

    async def _resolve_prices(
        self,
        model: Type[PriceHistoryIntraday] | Type[PriceHistoryDaily],
        provider_method_name: str,
    ):
        table_name = model.__tablename__
        logger.info(f"🔍 Starting resolution for {table_name}")

        async for session in get_db_session():
            try:
                # 1. Identify instruments needing resolution
                # We select distinct instrument_ids from the table where resolve_required is True
                # OR where any of the price fields (open, high, low, close) are 0 or NULL
                # AND datetime is in the past (less than current UTC time)
                # AND resolve_tries < 3
                now = datetime.now(timezone.utc)
                
                # Find min and max datetime for each instrument that needs resolution
                stmt = (
                    select(
                        model.instrument_id,
                        func.min(model.datetime).label("min_dt"),
                        func.max(model.datetime).label("max_dt")
                    )
                    .where(
                        model.datetime < now,
                        model.resolve_tries < 3,
                        (model.resolve_required.is_(True)) |
                        (model.open == 0) | (model.open.is_(None)) |
                        (model.high == 0) | (model.high.is_(None)) |
                        (model.low == 0) | (model.low.is_(None)) |
                        (model.close == 0) | (model.close.is_(None))
                    )
                    .group_by(model.instrument_id)
                )
                result = await session.execute(stmt)
                rows = result.all()

                if not rows:
                    logger.info(f"🔍 No records found needing resolution for {table_name}")
                    return

                # Map instrument_id -> (min_dt, max_dt)
                instrument_ranges = {row.instrument_id: (row.min_dt, row.max_dt) for row in rows}
                instrument_ids = list(instrument_ranges.keys())

                logger.info(
                    f"🔍 Found {len(instrument_ids)} instruments needing resolution in {table_name}"
                )

                # 2. Fetch Instrument objects
                stmt_instruments = (
                    select(Instrument)
                    .options(joinedload(Instrument.exchange))
                    .where(Instrument.id.in_(instrument_ids))
                )
                result_instruments = await session.execute(stmt_instruments)
                instruments = result_instruments.scalars().all()

                # Detach from session to avoid lazy loading issues if we just need the data we have
                # But we need to make sure we have the data first.
                # We need exchange info now.
                for i in instruments:
                    # Access attributes to ensure they are loaded
                    _ = i.symbol
                    _ = i.exchange_id
                    _ = i.exchange.timezone # Ensure timezone is loaded
                    session.expunge(i) # Detach to prevent implicit refresh errors

                # Now we can pass them.

                # 3. Group by provider AND timezone
                # We need to group by timezone because we might need to convert UTC ranges
                # to local dates for the provider.

                # Key: (provider_code, timezone_name)
                instruments_by_group = defaultdict(list)

                for instrument in instruments:
                    provider_code = self.provider_manager.exchange_to_provider.get(
                        instrument.exchange_id
                    )
                    if provider_code:
                        tz_name = instrument.exchange.timezone or "UTC"
                        instruments_by_group[(provider_code, tz_name)].append(instrument)
                    else:
                        logger.warning(
                            f"No provider mapped for exchange {instrument.exchange_id} (Instrument: {instrument.symbol})"
                        )

                # 4. Fetch and Update
                for (provider_code, tz_name), provider_instruments in instruments_by_group.items():
                    provider = self.provider_manager.providers.get(provider_code)
                    if not provider:
                        logger.warning(
                            f"Provider {provider_code} not initialized or found"
                        )
                        continue

                    # Group instruments by date range to optimize calls
                    # For simplicity, we can take the global min and max for this batch,
                    # or group by similar ranges.
                    # A simple robust approach:
                    # Find the overall min start_date and max end_date for this provider's batch.
                    # This might fetch more data than needed for some instruments, but reduces API calls.

                    # However, if ranges are vastly different (e.g. one needs 2020, one needs 2024),
                    # fetching 2020-2024 for all is bad.
                    # Let's try to group by "weeks" or just process individually if the provider supports batching?
                    # Most providers (like YF) take a list of tickers and a single start/end.

                    # Strategy: Calculate the union of all needed ranges.
                    # If the union is too large, we might need to split.
                    # For now, let's find the min start and max end for ALL instruments in this provider batch.

                    batch_min_dt = min(instrument_ranges[i.id][0] for i in provider_instruments)
                    batch_max_dt = max(instrument_ranges[i.id][1] for i in provider_instruments)

                    # Ensure we don't fetch future data
                    if batch_max_dt > now:
                        batch_max_dt = now

                    logger.info(
                        f"🔍 Requesting {provider_method_name} from {provider_code} "
                        f"for {len(provider_instruments)} instruments ({tz_name}). "
                        f"UTC Range: {batch_min_dt} to {batch_max_dt}"
                    )

                    # Call provider method dynamically
                    fetch_method = getattr(provider, provider_method_name)

                    # Ensure dates are timezone aware (UTC)
                    if batch_min_dt.tzinfo is None:
                        batch_min_dt = batch_min_dt.replace(tzinfo=timezone.utc)
                    if batch_max_dt.tzinfo is None:
                        batch_max_dt = batch_max_dt.replace(tzinfo=timezone.utc)

                    # NOTE: We pass UTC datetimes to the provider.
                    # The provider implementation is responsible for converting these to
                    # the required format (e.g. local date string) if necessary,
                    # using the instrument's exchange timezone if needed.
                    # However, BaseMarketDataProvider interface doesn't take timezone info explicitly,
                    # but it takes Instrument objects which have Exchange info.

                    fetched_data = await fetch_method(
                        instruments=provider_instruments,
                        start_date=batch_min_dt,
                        end_date=batch_max_dt
                    )

                    if not fetched_data:
                        logger.warning(f"🔍 No data returned from {provider_code}")
                        continue

                    # Calculate stats
                    total_symbols_returned = len(fetched_data)
                    total_records_returned = sum(len(records) for records in fetched_data.values())

                    logger.info(
                        f"🔍 Received data from {provider_code}. "
                        f"Symbols with data: {total_symbols_returned}/{len(provider_instruments)}. "
                        f"Total records: {total_records_returned}."
                    )

                    # 5. Update Database (Optimized Bulk Upsert)
                    logger.info(f"🔍 Saving {total_records_returned} records to database...")

                    # Map symbol to instrument_id for quick lookup
                    symbol_to_id = {i.symbol: i.id for i in provider_instruments}

                    # Flatten all records and assign correct instrument_id
                    all_records = []
                    for symbol, records in fetched_data.items():
                        if symbol not in symbol_to_id:
                            logger.warning(f"Received data for unknown symbol {symbol} from {provider_code}")
                            continue

                        inst_id = symbol_to_id[symbol]
                        for r in records:
                            r.instrument_id = inst_id # Assign correct ID
                            all_records.append(r)

                    if not all_records:
                        continue

                    # Prepare data for bulk operations
                    # We need to check which records exist to decide between UPDATE and INSERT
                    # For 7200 records, checking existence might be heavy if done one by one.
                    # We can try to use Postgres ON CONFLICT if we are sure about unique constraints.
                    # But to be safe and generic (and since we are resolving gaps), we can try a bulk approach.

                    # Strategy:
                    # 1. Extract all (instrument_id, datetime) pairs
                    # 2. Query DB to find which ones exist
                    # 3. Split into updates and inserts
                    # 4. Execute bulk update and bulk insert

                    # However, querying 7200 pairs might be slow too.
                    # Let's try to use the `resolve_required` flag.
                    # We assume we are updating records where `resolve_required=True` OR inserting new ones.
                    # But we might be updating records that exist but resolve_required=False (re-fetching).

                    # Let's use a temporary table or VALUES clause to check existence efficiently?
                    # Or just use `dialects.postgresql.insert` with `on_conflict_do_update`.
                    # Since we are on Neon (Postgres), this is the best way.
                    # We assume there is a UNIQUE constraint on (instrument_id, datetime).
                    # If not, we might get duplicates on INSERT.

                    from sqlalchemy.dialects.postgresql import insert as pg_insert

                    # Convert objects to dicts and deduplicate
                    # We use a dict keyed by (instrument_id, datetime) to ensure uniqueness within the batch
                    # This prevents "ON CONFLICT DO UPDATE command cannot affect row a second time" error
                    unique_records = {}
                    for r in all_records:
                        key = (r.instrument_id, r.datetime)
                        d = {
                            "instrument_id": r.instrument_id,
                            "datetime": r.datetime,
                            "open": r.open,
                            "high": r.high,
                            "low": r.low,
                            "close": r.close,
                            "volume": r.volume,
                            "resolve_required": False
                        }
                        if hasattr(r, "adj_close"):
                            d["adj_close"] = r.adj_close
                        unique_records[key] = d

                    records_dicts = list(unique_records.values())

                    # Sort records by instrument_id and datetime to prevent deadlocks
                    records_dicts.sort(key=lambda x: (x['instrument_id'], x['datetime']))

                    # Chunking to avoid huge statements
                    chunk_size = 1000
                    total_updated = 0

                    for i in range(0, len(records_dicts), chunk_size):
                        chunk = records_dicts[i:i + chunk_size]

                        stmt = pg_insert(model).values(chunk)

                        # Define update columns
                        update_dict = {
                            "open": stmt.excluded.open,
                            "high": stmt.excluded.high,
                            "low": stmt.excluded.low,
                            "close": stmt.excluded.close,
                            "volume": stmt.excluded.volume,
                            "resolve_required": False
                        }
                        if "adj_close" in chunk[0]:
                            update_dict["adj_close"] = stmt.excluded.adj_close

                        # ON CONFLICT DO UPDATE
                        # We need to specify the constraint.
                        # If we don't know the name, we can specify index_elements.
                        # Assuming (instrument_id, datetime) is unique.
                        stmt = stmt.on_conflict_do_update(
                            index_elements=['instrument_id', 'datetime'],
                            set_=update_dict
                        )

                        result = await session.execute(stmt)
                        total_updated += result.rowcount

                    await session.commit()
                    logger.info(f"🔍 Updated/Inserted {total_updated} records for {provider_code}")

            except Exception as e:
                logger.error(f"🔍 Error resolving prices for {table_name}: {e}", exc_info=True)

    async def resolve_specific_records(self, records: List[PriceHistoryIntraday]):
        """
        Immediately resolve a specific list of PriceHistoryIntraday records.
        This is useful for real-time gap filling when data is missing during ingestion.
        """
        if not records:
            return

        logger.info(f"🔍 resolving {len(records)} specific records immediately.")

        # Group records by instrument_id to fetch instrument details efficiently
        instrument_ids = list({r.instrument_id for r in records})

        async for session in get_db_session():
            try:
                # Fetch Instrument objects with Exchange info
                stmt = (
                    select(Instrument)
                    .options(joinedload(Instrument.exchange))
                    .where(Instrument.id.in_(instrument_ids))
                )
                result = await session.execute(stmt)
                instruments = result.scalars().all()

                # Map ID to Instrument
                instrument_map = {i.id: i for i in instruments}

                # Detach instruments from session
                for i in instruments:
                    _ = i.symbol
                    _ = i.exchange_id
                    _ = i.exchange.timezone
                    session.expunge(i)

                # Group by provider and timezone
                # Key: (provider_code, timezone_name)
                instruments_by_group = defaultdict(list)

                # Also map instrument_id to the list of records we want to resolve for it
                # This helps us determine the date range needed for each instrument
                records_by_instrument = defaultdict(list)
                for r in records:
                    records_by_instrument[r.instrument_id].append(r)

                for instrument in instruments:
                    provider_code = self.provider_manager.exchange_to_provider.get(
                        instrument.exchange_id
                    )
                    if provider_code:
                        tz_name = instrument.exchange.timezone or "UTC"
                        instruments_by_group[(provider_code, tz_name)].append(instrument)

                # Process each group
                for (provider_code, tz_name), provider_instruments in instruments_by_group.items():
                    provider = self.provider_manager.providers.get(provider_code)
                    if not provider:
                        continue

                    # Determine the overall min/max date range for this batch
                    # We look at the specific records we want to resolve
                    batch_min_dt = None
                    batch_max_dt = None

                    for inst in provider_instruments:
                        inst_records = records_by_instrument[inst.id]
                        if not inst_records:
                            continue

                        min_dt = min(r.datetime for r in inst_records)
                        max_dt = max(r.datetime for r in inst_records)

                        if batch_min_dt is None or min_dt < batch_min_dt:
                            batch_min_dt = min_dt
                        if batch_max_dt is None or max_dt > batch_max_dt:
                            batch_max_dt = max_dt

                    if batch_min_dt is None:
                        continue

                    # Ensure dates are timezone aware (UTC)
                    if batch_min_dt.tzinfo is None:
                        batch_min_dt = batch_min_dt.replace(tzinfo=timezone.utc)
                    if batch_max_dt.tzinfo is None:
                        batch_max_dt = batch_max_dt.replace(tzinfo=timezone.utc)

                    logger.info(
                        f"🔍 Requesting immediate update from {provider_code} "
                        f"for {len(provider_instruments)} instruments. "
                        f"UTC Range: {batch_min_dt} to {batch_max_dt}"
                    )

                    # Fetch data
                    # We use get_intraday_prices as we are dealing with PriceHistoryIntraday
                    fetched_data = await provider.get_intraday_prices(
                        instruments=provider_instruments,
                        start_date=batch_min_dt,
                        end_date=batch_max_dt
                    )

                    if not fetched_data:
                        logger.warning(f"🔍 No data returned from {provider_code} for immediate resolution.")
                        continue

                    total_fetched = sum(len(v) for v in fetched_data.values())
                    logger.info(f"🔍 Fetched {total_fetched} records for {len(fetched_data)} symbols from {provider_code}.")

                    # Save to DB
                    # We reuse the logic from _resolve_prices but adapted for this context
                    # Since we have the records in hand (passed as argument), we could update them directly?
                    # But 'records' might be detached or transient objects.
                    # It's safer to do a bulk upsert into the DB to ensure persistence.

                    # Map symbol to instrument_id
                    symbol_to_id = {i.symbol: i.id for i in provider_instruments}

                    all_records = []
                    for symbol, fetched_records in fetched_data.items():
                        if symbol not in symbol_to_id:
                            continue
                        inst_id = symbol_to_id[symbol]
                        for r in fetched_records:
                            r.instrument_id = inst_id
                            all_records.append(r)

                    if not all_records:
                        continue

                    from sqlalchemy.dialects.postgresql import insert as pg_insert

                    unique_records = {}
                    for r in all_records:
                        key = (r.instrument_id, r.datetime)
                        d = {
                            "instrument_id": r.instrument_id,
                            "datetime": r.datetime,
                            "open": r.open,
                            "high": r.high,
                            "low": r.low,
                            "close": r.close,
                            "volume": r.volume,
                            "resolve_required": False
                        }
                        unique_records[key] = d

                    records_dicts = list(unique_records.values())
                    records_dicts.sort(key=lambda x: (x['instrument_id'], x['datetime']))

                    chunk_size = 1000
                    total_updated = 0

                    for i in range(0, len(records_dicts), chunk_size):
                        chunk = records_dicts[i:i + chunk_size]
                        stmt = pg_insert(PriceHistoryIntraday).values(chunk)

                        update_dict = {
                            "open": stmt.excluded.open,
                            "high": stmt.excluded.high,
                            "low": stmt.excluded.low,
                            "close": stmt.excluded.close,
                            "volume": stmt.excluded.volume,
                            "resolve_required": False
                        }

                        stmt = stmt.on_conflict_do_update(
                            index_elements=['instrument_id', 'datetime'],
                            set_=update_dict
                        )

                        result = await session.execute(stmt)
                        total_updated += result.rowcount

                    await session.commit()
                    logger.info(f"🔍 Immediate resolution updated {total_updated} records.")

            except Exception as e:
                logger.error(f"🔍 Error in resolve_specific_records: {e}", exc_info=True)
