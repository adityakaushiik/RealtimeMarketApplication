import asyncio
import time
import pytz
from datetime import datetime, timedelta, timezone
from typing import List, Optional, Dict
from sqlalchemy import select, update, func
from sqlalchemy.ext.asyncio import AsyncSession

from services.redis_timeseries import get_redis_timeseries
from services.provider.provider_manager import get_provider_manager
from services.data.data_resolver import DataResolver
from models.exchange import Exchange
from models.instruments import Instrument
from models.price_history_intraday import PriceHistoryIntraday
from models.price_history_daily import PriceHistoryDaily
from config.database_config import get_db_session
from config.logger import logger
from config.redis_config import get_redis
from utils.data_validation import validate_ohlc, validate_volume


class DataSaver:
    """
    DataSaver handles periodic updates of market data from Redis TimeSeries to the database.
    It updates existing records created by DataCreationService.
    """

    def __init__(self):
        self.redis_timeseries = get_redis_timeseries()
        self.redis = get_redis()
        self.exchanges: List[Exchange] = []
        self._running_tasks: Dict[str, asyncio.Task] = {}
        self._stop_flags: Dict[str, bool] = {}
        # Cache stores symbol -> (instrument_id, should_record_data, exchange_id)
        self.symbol_map_cache: Dict[str, tuple[int, bool, int]] = {}
        self.provider_manager = get_provider_manager()
        self.data_resolver = DataResolver(self.provider_manager)

    def add_exchange(self, exchange: Exchange) -> None:
        """Add an exchange to monitor."""
        # Initialize timestamps for the exchange
        tz = pytz.timezone(exchange.timezone)
        exchange.update_timestamps_for_date(datetime.now(tz).date())
        
        self.exchanges.append(exchange)
        logger.info(f"Added exchange: {exchange.name}")

    async def start_all_exchanges(self, interval_minutes: Optional[int] = None) -> None:
        """Start periodic updates for all exchanges."""
        if not self.exchanges:
            logger.warning("No exchanges registered")
            return

        for exchange in self.exchanges:
            task = asyncio.create_task(
                self.run_periodic_save(exchange, interval_minutes)
            )
            self._running_tasks[exchange.name] = task

        logger.info(f"Started periodic updates for {len(self.exchanges)} exchange(s)")

    async def stop_all_exchanges(self) -> None:
        """Stop all running periodic updates."""
        for exchange_name in self._running_tasks:
            self._stop_flags[exchange_name] = True

        if self._running_tasks:
            await asyncio.gather(*self._running_tasks.values(), return_exceptions=True)

        self._running_tasks.clear()
        self._stop_flags.clear()
        logger.info("Stopped all periodic updates")

    async def run_periodic_save(
        self, exchange: Exchange, interval_minutes: Optional[int] = None
    ) -> None:
        """
        Run periodic updates for a specific exchange.
        """
        exchange_name = exchange.name
        interval = (
            interval_minutes
            if interval_minutes is not None
            else exchange.interval_minutes
        )
        interval_ms = interval * 60 * 1000

        logger.info(
            f"Starting periodic update for {exchange_name} (Interval: {interval}m)"
        )

        tz = pytz.timezone(exchange.timezone)

        try:
            while not self._stop_flags.get(exchange_name, False):
                current_time_ms = int(time.time() * 1000)
                logger.debug(
                    f"[{exchange_name}] Check: Current={current_time_ms}, Start={exchange.start_time}, End={exchange.end_time}"
                )

                # Check if market is open or if we should do a final save
                if current_time_ms > exchange.end_time + interval_ms:
                    # Market closed for the current configured day.
                    # Check if we need to roll over to tomorrow or if we just need to update to today (if stale).
                    current_date = datetime.now(tz).date()

                    # Update to current date first
                    exchange.update_timestamps_for_date(current_date)

                    # Check again
                    if current_time_ms > exchange.end_time + interval_ms:
                        # Still closed for today, so move to tomorrow
                        next_date = current_date + timedelta(days=1)
                        exchange.update_timestamps_for_date(next_date)
                        logger.info(
                            f"Market closed for {exchange_name}. Waiting for next session on {next_date}..."
                        )
                    else:
                        logger.info(
                            f"Updated schedule for {exchange_name} to today {current_date}"
                        )

                    # Continue to loop to hit the wait block
                    continue

                if current_time_ms < exchange.start_time:
                    wait_ms = exchange.start_time - current_time_ms
                    logger.info(f"Waiting {wait_ms / 1000:.0f}s for market open...")
                    await asyncio.sleep(wait_ms / 1000)
                    continue

                # Calculate alignment to the next interval
                # We want to run slightly after the interval closes to ensure data is available
                next_interval_ts = ((current_time_ms // interval_ms) + 1) * interval_ms
                wait_ms = next_interval_ts - current_time_ms + 5000  # Add 5s buffer

                logger.info(
                    f"[{exchange_name}] Sleeping for {wait_ms / 1000:.1f}s until next interval"
                )
                await asyncio.sleep(wait_ms / 1000)

                if self._stop_flags.get(exchange_name, False):
                    break

                logger.info(f"[{exchange_name}] Woke up. Updating records...")
                # We want the bucket that just finished, so we subtract one interval
                # e.g. if we woke up at 10:05:05 (target 10:05:00), we want the 10:00:00 bucket
                target_bucket_ts = next_interval_ts - interval_ms
                await self.update_records_for_interval(
                    exchange, interval, target_bucket_ts
                )

        except asyncio.CancelledError:
            logger.info(f"Periodic update cancelled for {exchange_name}")
        except Exception as e:
            logger.error(
                f"Fatal error in periodic update for {exchange_name}: {e}",
                exc_info=True,
            )

    async def update_records_for_interval(
        self, exchange: Exchange, interval_minutes: int, align_to_ts: int
    ) -> None:
        """Fetch data from Redis and update existing DB records."""
        try:
            keys = await self.redis_timeseries.get_all_keys()
            if not keys:
                logger.warning(
                    f"[{exchange.name}] No keys found in Redis"
                )
                return

            logger.info(
                f"[{exchange.name}] Found {len(keys)} keys. Fetching OHLCV aligned to {align_to_ts}..."
            )

            # Fetch OHLCV data for the last interval
            # We want the bucket starting at align_to_ts (e.g. 13:10).
            # get_ohlcv_window looks backwards from the provided timestamp.
            # So we need to align to the END of the bucket (e.g. 13:15) and look back 5 minutes.
            query_align_ts = align_to_ts + (interval_minutes * 60 * 1000)

            tasks = [
                self.redis_timeseries.get_ohlcv_window(
                    key,
                    window_minutes=interval_minutes,
                    bucket_minutes=interval_minutes,
                    align_to_ts=query_align_ts,
                )
                for key in keys
            ]
            results = await asyncio.gather(*tasks)

            valid_data = {}
            missing_data_symbols = []
            for key, result in zip(keys, results):
                # Check if we have valid data
                if result and result.get("ts"):
                    valid_data[key] = result
                else:
                    missing_data_symbols.append(key)

            if missing_data_symbols:
                logger.warning(
                    f"[{exchange.name}] No data in window for {len(missing_data_symbols)} symbols. Examples: {missing_data_symbols[:5]}"
                )

                # Trigger immediate resolution for missing symbols
                # We need to map these symbols to instrument IDs first
                async for session in get_db_session():
                    symbol_map = await self._get_symbol_to_instrument_mapping(
                        session, missing_data_symbols, exchange.id
                    )

                    records_to_resolve = []
                    for sym in missing_data_symbols:
                        inst_id = symbol_map.get(sym)
                        if inst_id:
                            # Create a dummy record with the target timestamp to indicate what we need
                            # The timestamp is the START of the bucket we missed
                            # The query_align_ts is the END of the bucket.
                            # So start is query_align_ts - interval_ms
                            interval_ms = interval_minutes * 60 * 1000
                            bucket_start_ts = query_align_ts - interval_ms
                            record_dt = datetime.fromtimestamp(bucket_start_ts / 1000, tz=timezone.utc)

                            records_to_resolve.append(
                                PriceHistoryIntraday(
                                    instrument_id=inst_id,
                                    datetime=record_dt
                                )
                            )

                    if records_to_resolve:
                        logger.info(f"[{exchange.name}] Triggering immediate resolution for {len(records_to_resolve)} missing symbols. Timestamp: {records_to_resolve[0].datetime}")
                        logger.debug(f"[{exchange.name}] Resolving symbols: {[sym for sym in missing_data_symbols if symbol_map.get(sym)]}")
                        # Run in background to not block the main loop too much?
                        # Or run inline to ensure data is available ASAP?
                        # Inline is safer for data consistency but might delay next cycle.
                        # Given it's "missing data", delay is acceptable to fix it.
                        await self.data_resolver.resolve_specific_records(records_to_resolve)

            if not valid_data:
                logger.warning(
                    f"[{exchange.name}] No valid data found for interval"
                )
                return

            logger.info(
                f"[{exchange.name}] Updating DB for {len(valid_data)} symbols..."
            )

            async for session in get_db_session():
                try:
                    symbol_map = await self._get_symbol_to_instrument_mapping(
                        session, list(valid_data.keys()), exchange.id
                    )

                    unmapped_symbols = []
                    for symbol in valid_data.keys():
                        if symbol not in symbol_map:
                            unmapped_symbols.append(symbol)

                    if unmapped_symbols:
                        logger.warning(f"[{exchange.name}] {len(unmapped_symbols)} symbols have data but no instrument mapping (or not marked for recording). Examples: {unmapped_symbols[:5]}")

                    # Sort items by instrument_id to ensure consistent locking order and prevent deadlocks
                    valid_items = []
                    for symbol, data in valid_data.items():
                        inst_id = symbol_map.get(symbol)
                        if inst_id:
                            valid_items.append((inst_id, symbol, data))

                    # Sort by instrument_id
                    valid_items.sort(key=lambda x: x[0])

                    for instrument_id, symbol, data in valid_items:
                        # Extract scalar values
                        ts = data["ts"]
                        open_val = data["open"]
                        high_val = data["high"]
                        low_val = data["low"]
                        close_val = data["close"]
                        vol_val = (
                            int(data["volume"]) if data["volume"] is not None else 0
                        )
                        tick_count = data.get("count", 0)

                        if open_val is None:  # Skip if no data in this bucket
                            continue

                        # C2: Validate and fix OHLC data
                        ohlc_result = validate_ohlc(open_val, high_val, low_val, close_val, vol_val, fix=True)
                        if not ohlc_result['valid'] and ohlc_result['fixed']:
                            logger.warning(f"[{exchange.name}] Fixed OHLC for {symbol}: {ohlc_result['errors']}")
                            open_val = ohlc_result['data']['open']
                            high_val = ohlc_result['data']['high']
                            low_val = ohlc_result['data']['low']
                            close_val = ohlc_result['data']['close']

                        # B4: Validate volume
                        vol_result = validate_volume(vol_val)
                        if vol_result['warnings']:
                            logger.warning(f"[{exchange.name}] Volume warning for {symbol}: {vol_result['warnings']}")
                        vol_val = vol_result['volume']

                        record_dt = datetime.fromtimestamp(ts / 1000, tz=timezone.utc)

                        # Log tick count for monitoring data quality
                        if tick_count < 10:
                            logger.warning(f"[{exchange.name}] Low tick count for {symbol} at {record_dt}: {tick_count} ticks")

                        # Update Intraday Record
                        stmt = (
                            update(PriceHistoryIntraday)
                            .where(
                                PriceHistoryIntraday.instrument_id == instrument_id,
                                PriceHistoryIntraday.datetime == record_dt,
                            )
                            .values(
                                open=open_val,
                                high=high_val,
                                low=low_val,
                                close=close_val,
                                volume=vol_val,
                                resolve_required=False,
                            )
                        )
                        result = await session.execute(stmt)
                        if result.rowcount == 0:
                            # Upsert: Insert if update failed (record doesn't exist)
                            logger.info(
                                f"Creating new intraday record for {symbol} at {record_dt}"
                            )
                            new_record = PriceHistoryIntraday(
                                instrument_id=instrument_id,
                                datetime=record_dt,
                                open=open_val,
                                high=high_val,
                                low=low_val,
                                close=close_val,
                                volume=vol_val,
                                resolve_required=False,
                            )
                            session.add(new_record)

                        # Update Daily Record
                        record_dt_local = record_dt.astimezone(
                            pytz.timezone(exchange.timezone)
                        )

                        daily_dt = datetime.fromtimestamp(
                            exchange._compute_timestamp(
                                record_dt_local.date(), exchange.market_open_time
                            )
                            / 1000,
                            tz=timezone.utc,
                        )

                        daily_stmt = select(PriceHistoryDaily).where(
                            PriceHistoryDaily.instrument_id == instrument_id,
                            PriceHistoryDaily.datetime == daily_dt,
                        )
                        daily_result = await session.execute(daily_stmt)
                        daily_record = daily_result.scalar_one_or_none()

                        if not daily_record:
                            # Upsert: Create daily record if it doesn't exist
                            logger.info(
                                f"Creating new daily record for {symbol} at {daily_dt}"
                            )
                            daily_record = PriceHistoryDaily(
                                instrument_id=instrument_id,
                                datetime=daily_dt,
                                open=open_val,
                                high=high_val,
                                low=low_val,
                                close=close_val,
                                volume=0, # Will be updated below
                                resolve_required=False,
                            )
                            session.add(daily_record)

                        # Update Daily Record values
                        daily_record.resolve_required = False
                        daily_record.close = close_val

                        if daily_record.open is None:
                            daily_record.open = open_val

                        if (
                            daily_record.high is None
                            or high_val > daily_record.high
                        ):
                            daily_record.high = high_val

                        if daily_record.low is None or low_val < daily_record.low:
                            daily_record.low = low_val

                        # Recalculate daily volume from intraday records to ensure accuracy and avoid double counting
                        day_start = daily_dt
                        day_end = day_start + timedelta(days=1)

                        # We need to flush the session to ensure the new intraday record is visible for the sum query
                        await session.flush()

                        vol_stmt = select(func.sum(PriceHistoryIntraday.volume)).where(
                            PriceHistoryIntraday.instrument_id == instrument_id,
                            PriceHistoryIntraday.datetime >= day_start,
                            PriceHistoryIntraday.datetime < day_end
                        )
                        vol_result = await session.execute(vol_stmt)
                        total_vol = vol_result.scalar() or 0

                        daily_record.volume = total_vol
                        session.add(daily_record)

                    await session.commit()

                    # Update last save time in Redis
                    if self.redis:
                        try:
                            await self.redis.set(f"last_save_time:{interval_minutes}m", str(align_to_ts))
                            logger.info(f"Updated last_save_time:{interval_minutes}m to {align_to_ts}")
                        except Exception as e:
                            logger.error(f"Error updating last_save_time in Redis: {e}")

                except Exception as e:
                    logger.error(f"Error saving data for {exchange.name}: {e}", exc_info=True)
                    await session.rollback()

        except Exception as e:
            logger.error(f"Error in update_records_for_interval for {exchange.name}: {e}", exc_info=True)

    async def _get_symbol_to_instrument_mapping(
        self, session: AsyncSession, symbols: List[str], exchange_id: int
    ) -> Dict[str, int]:
        mapping = {}
        symbols_to_fetch = []

        # Check cache first
        for sym in symbols:
            if sym in self.symbol_map_cache:
                inst_id, should_record, ex_id = self.symbol_map_cache[sym]
                if ex_id == exchange_id and should_record:
                    mapping[sym] = inst_id
            else:
                symbols_to_fetch.append(sym)

        if symbols_to_fetch:
            stmt = select(Instrument).where(
                Instrument.symbol.in_(symbols_to_fetch),
                Instrument.exchange_id == exchange_id
            )
            result = await session.execute(stmt)
            instruments = result.scalars().all()

            for inst in instruments:
                self.symbol_map_cache[inst.symbol] = (inst.id, inst.should_record_data, inst.exchange_id)
                if inst.should_record_data:
                    mapping[inst.symbol] = inst.id

        return mapping

