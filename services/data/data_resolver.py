from collections import defaultdict
from datetime import datetime, timezone
from typing import Type

from sqlalchemy import select, update, bindparam, inspect, text
from sqlalchemy.orm import load_only

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
                stmt = (
                    update(PriceHistoryIntraday)
                    .where(
                        PriceHistoryIntraday.datetime > last_save_dt,
                        PriceHistoryIntraday.datetime < now,
                        PriceHistoryIntraday.instrument_id.in_(instrument_ids)
                    )
                    .values(resolve_required=True)
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
        """
        logger.info("Starting aggregation of intraday prices to daily for today")

        today_start = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)

        async for session in get_db_session():
            try:
                # 1. Find daily records for today that need resolution
                stmt = select(PriceHistoryDaily).where(
                    PriceHistoryDaily.resolve_required == True,
                    PriceHistoryDaily.datetime >= today_start
                )
                result = await session.execute(stmt)
                daily_records = result.scalars().all()

                if not daily_records:
                    logger.info("No daily records for today needing resolution via aggregation.")
                    return

                logger.info(f"Found {len(daily_records)} daily records for today to aggregate from intraday.")

                # Map instrument_id to daily_record for easy update
                daily_map = {r.instrument_id: r for r in daily_records}
                instrument_ids = list(daily_map.keys())

                # 2. Aggregate intraday data for all these instruments in one query
                # We use a subquery or window functions to get first/last efficiently
                # Since we are on TimescaleDB, we can use `first()` and `last()` if the extension is active,
                # but to be safe and portable within standard SQL/SQLAlchemy:

                # We'll use a common pattern: Group by instrument_id and get min/max/sum
                # For Open/Close, we can use `first_value` / `last_value` window functions or `distinct on` in Postgres.
                # Here is a robust Postgres approach using `DISTINCT ON` isn't great for aggregation.
                # Let's use the `first` and `last` from TimescaleDB if possible, but assuming standard Postgres:

                # We will fetch the aggregates (High, Low, Volume) and then Open/Close separately or
                # use a more complex query.
                # Given the requirement for efficiency, let's try to do it in one go using a CTE or similar.

                # However, for simplicity and reliability with SQLAlchemy ORM:
                # We can query for all intraday records for these instruments today, ordered by time.
                # But that's too much data.

                # Let's use a custom SQL query for efficiency.

                # Construct the query
                # We assume 'price_history_intraday' is the table name
                # We want: instrument_id, first(open), max(high), min(low), last(close), sum(volume)
                # TimescaleDB provides first(value, time) and last(value, time)

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
                    GROUP BY instrument_id
                """)

                # If first/last are not available (standard Postgres), we would need a different query.
                # Assuming TimescaleDB is enabled as per prompt "price_history_intraday is a timescale db hypertable".

                result = await session.execute(query, {
                    "instrument_ids": instrument_ids,
                    "start_time": today_start
                })

                rows = result.fetchall()

                updated_count = 0
                for row in rows:
                    inst_id = row.instrument_id
                    if inst_id in daily_map:
                        record = daily_map[inst_id]
                        record.open = row.open
                        record.high = row.high
                        record.low = row.low
                        record.close = row.close
                        record.volume = row.volume
                        record.resolve_required = False
                        session.add(record)
                        updated_count += 1

                await session.commit()
                logger.info(f"Successfully aggregated {updated_count} daily records from intraday data.")

            except Exception as e:
                logger.error(f"Error aggregating intraday to daily: {e}", exc_info=True)

    async def _resolve_prices(
        self,
        model: Type[PriceHistoryIntraday] | Type[PriceHistoryDaily],
        provider_method_name: str,
    ):
        table_name = model.__tablename__
        logger.info(f"Starting resolution for {table_name}")

        async for session in get_db_session():
            try:
                # 1. Identify instruments needing resolution
                # We select distinct instrument_ids from the table where resolve_required is True
                # OR where any of the price fields (open, high, low, close) are 0 or NULL
                # AND datetime is in the past (less than current UTC time)
                now = datetime.now(timezone.utc)
                
                stmt = (
                    select(model.instrument_id, model.datetime)
                    .where(
                        model.datetime < now,
                        (model.resolve_required.is_(True)) |
                        (model.open == 0) | (model.open.is_(None)) |
                        (model.high == 0) | (model.high.is_(None)) |
                        (model.low == 0) | (model.low.is_(None)) |
                        (model.close == 0) | (model.close.is_(None))
                    )
                )
                result = await session.execute(stmt)
                rows = result.all()

                if not rows:
                    logger.info(f"No records found needing resolution for {table_name}")
                    return

                # Create a set of (instrument_id, datetime) for fast lookup
                # We normalize datetime to ensure timezone consistency if needed, but usually DB and provider should match
                unresolved_keys = set()
                instrument_ids = set()
                for r in rows:
                    instrument_ids.add(r.instrument_id)
                    # Ensure we store as UTC or naive depending on DB, usually DB returns with TZ if column is TZ aware
                    unresolved_keys.add((r.instrument_id, r.datetime))

                logger.info(
                    f"Found {len(instrument_ids)} instruments with {len(unresolved_keys)} records needing resolution in {table_name}"
                )

                # 2. Fetch Instrument objects
                stmt_instruments = select(Instrument).where(
                    Instrument.id.in_(instrument_ids)
                )
                result_instruments = await session.execute(stmt_instruments)
                instruments = result_instruments.scalars().all()

                # Detach from session to avoid lazy loading issues if we just need the data we have
                # But we need to make sure we have the data first.
                for i in instruments:
                    # Access attributes to ensure they are loaded
                    _ = i.symbol
                    _ = i.exchange_id

                # Now we can pass them.

                # 3. Group by provider
                instruments_by_provider = defaultdict(list)
                for instrument in instruments:
                    provider_code = self.provider_manager.exchange_to_provider.get(
                        instrument.exchange_id
                    )
                    if provider_code:
                        instruments_by_provider[provider_code].append(instrument)
                    else:
                        logger.warning(
                            f"No provider mapped for exchange {instrument.exchange_id} (Instrument: {instrument.symbol})"
                        )

                # 4. Fetch and Update
                for provider_code, provider_instruments in instruments_by_provider.items():
                    provider = self.provider_manager.providers.get(provider_code)
                    if not provider:
                        logger.warning(
                            f"Provider {provider_code} not initialized or found"
                        )
                        continue

                    logger.info(
                        f"Fetching data from {provider_code} for {len(provider_instruments)} instruments"
                    )

                    try:
                        fetch_method = getattr(provider, provider_method_name)
                        # This returns dict[symbol, list[Model]]

                        # Ensure fetch_method is awaited properly, handling potential sync/async mismatch or context issues
                        # The error "greenlet_spawn has not been called" suggests an async DB operation inside a sync context
                        # or vice versa, but here we are in an async function awaiting an async provider method.
                        # However, if the provider method does DB operations (lazy loading), it might trigger this.
                        # YahooProvider uses yfinance which is sync, wrapped in asyncio.to_thread.
                        # But if it accesses instrument attributes that are lazy loaded, it might fail if not eager loaded.

                        fetched_data = await fetch_method(provider_instruments)

                        update_params = []

                        for symbol, history_list in fetched_data.items():
                            # Find instrument id
                            instrument = next(
                                (i for i in provider_instruments if i.symbol == symbol),
                                None,
                            )
                            if not instrument:
                                continue

                            for history_item in history_list:
                                # Check if this record actually needs resolution
                                # We need to match the datetime precision and timezone
                                # Assuming exact match for now. If provider returns naive and DB is aware, might need conversion.
                                # Usually both should be UTC aware or consistent.

                                if (instrument.id, history_item.datetime) not in unresolved_keys:
                                    continue

                                # Prepare update dict

                                param = {
                                    "b_instrument_id": instrument.id,
                                    "b_datetime": history_item.datetime,
                                    "open": history_item.open,
                                    "high": history_item.high,
                                    "low": history_item.low,
                                    "close": history_item.close,
                                    "volume": history_item.volume,
                                    "resolve_required": False,
                                }
                                
                                if model == PriceHistoryIntraday:
                                    param["interval"] = getattr(history_item, "interval", None)

                                update_params.append(param)

                        if update_params:
                            logger.info(
                                f"Updating {len(update_params)} records for {provider_code} in {table_name}"
                            )

                            # Use Core Table for bulk update to avoid ORM primary key requirement
                            table = inspect(model).local_table

                            values_dict = {
                                "open": bindparam("open"),
                                "high": bindparam("high"),
                                "low": bindparam("low"),
                                "close": bindparam("close"),
                                "volume": bindparam("volume"),
                                "resolve_required": bindparam("resolve_required"),
                            }
                            
                            if model == PriceHistoryIntraday:
                                values_dict["interval"] = bindparam("interval")

                            stmt_update = (
                                update(table)
                                .where(
                                    table.c.instrument_id == bindparam("b_instrument_id"),
                                    table.c.datetime == bindparam("b_datetime"),
                                )
                                .values(**values_dict)
                            )

                            await session.execute(stmt_update, update_params)
                            await session.commit()
                        else:
                            logger.info(f"No matching data found to update for {provider_code}")

                    except Exception as e:
                        logger.error(
                            f"Error resolving prices with {provider_code}: {e}",
                            exc_info=True,
                        )

            except Exception as e:
                logger.error(f"Error in _resolve_prices for {table_name}: {e}", exc_info=True)
