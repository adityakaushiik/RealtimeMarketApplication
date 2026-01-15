import asyncio
import time
import pytz
from datetime import datetime, timedelta, timezone
from datetime import date as DateType
from typing import List, Optional, Dict
from sqlalchemy import select, update, func
from sqlalchemy.ext.asyncio import AsyncSession

from services.redis_timeseries import get_redis_timeseries, RedisTimeSeries
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
    DataSaver handles end-of-day transfer of market data from Redis TimeSeries to TimescaleDB.

    New Strategy:
    - During trading hours: Data stays in Redis 5m downsampled keys (not written to DB)
    - After market close: Transfer all Redis 5m data to TimescaleDB for permanent storage
    - Historical queries use TimescaleDB; today's queries use Redis
    """

    def __init__(self):
        self.redis_timeseries: RedisTimeSeries = get_redis_timeseries()
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
                logger.warning(f"[{exchange.name}] No keys found in Redis")
                return

            logger.info(
                f"[{exchange.name}] Found {len(keys)} keys. Fetching OHLCV for last {interval_minutes} minutes..."
            )

            # Fetch the last N minutes of 5m candles from downsampled keys
            # For a 15m interval, we get the last 3 x 5m candles
            now_ts = int(time.time() * 1000)
            from_ts = align_to_ts  # Start of the period we want to save

            tasks = [
                self.redis_timeseries.get_5m_candles(
                    symbol=key,
                    from_ts=from_ts,
                    to_ts=now_ts,
                )
                for key in keys
            ]
            results = await asyncio.gather(*tasks)

            valid_data = {}
            missing_data_symbols = []
            for key, result in zip(keys, results):
                # Check if we have valid data (at least one candle)
                if result and result.get("timestamp") and len(result["timestamp"]) > 0:
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
                            record_dt = datetime.fromtimestamp(
                                bucket_start_ts / 1000, tz=timezone.utc
                            )

                            records_to_resolve.append(
                                PriceHistoryIntraday(
                                    instrument_id=inst_id, datetime=record_dt
                                )
                            )

                    if records_to_resolve:
                        logger.info(
                            f"[{exchange.name}] Triggering immediate resolution for {len(records_to_resolve)} missing symbols. Timestamp: {records_to_resolve[0].datetime}"
                        )
                        logger.debug(
                            f"[{exchange.name}] Resolving symbols: {[sym for sym in missing_data_symbols if symbol_map.get(sym)]}"
                        )
                        # Run in background to not block the main loop too much?
                        # Or run inline to ensure data is available ASAP?
                        # Inline is safer for data consistency but might delay next cycle.
                        # Given it's "missing data", delay is acceptable to fix it.
                        await self.data_resolver.resolve_specific_records(
                            records_to_resolve
                        )

            if not valid_data:
                logger.warning(f"[{exchange.name}] No valid data found for interval")
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
                        logger.warning(
                            f"[{exchange.name}] {len(unmapped_symbols)} symbols have data but no instrument mapping (or not marked for recording). Examples: {unmapped_symbols[:5]}"
                        )

                    # Sort items by instrument_id to ensure consistent locking order and prevent deadlocks
                    valid_items = []
                    for symbol, data in valid_data.items():
                        inst_id = symbol_map.get(symbol)
                        if inst_id:
                            valid_items.append((inst_id, symbol, data))

                    # Sort by instrument_id
                    valid_items.sort(key=lambda x: x[0])

                    for instrument_id, symbol, candles_data in valid_items:
                        # Process each candle in the list
                        timestamps = candles_data.get("timestamp", [])
                        opens = candles_data.get("open", [])
                        highs = candles_data.get("high", [])
                        lows = candles_data.get("low", [])
                        closes = candles_data.get("close", [])
                        volumes = candles_data.get("volume", [])

                        for i, ts in enumerate(timestamps):
                            open_val = opens[i] if i < len(opens) else None
                            high_val = highs[i] if i < len(highs) else None
                            low_val = lows[i] if i < len(lows) else None
                            close_val = closes[i] if i < len(closes) else None
                            vol_val = (
                                int(volumes[i])
                                if i < len(volumes) and volumes[i] is not None
                                else 0
                            )

                            if open_val is None:  # Skip if no data in this bucket
                                continue

                            # C2: Validate and fix OHLC data
                            ohlc_result = validate_ohlc(
                                open_val,
                                high_val,
                                low_val,
                                close_val,
                                vol_val,
                                fix=True,
                            )
                            if not ohlc_result["valid"] and ohlc_result["fixed"]:
                                logger.warning(
                                    f"[{exchange.name}] Fixed OHLC for {symbol}: {ohlc_result['errors']}"
                                )
                                open_val = ohlc_result["data"]["open"]
                                high_val = ohlc_result["data"]["high"]
                                low_val = ohlc_result["data"]["low"]
                                close_val = ohlc_result["data"]["close"]

                            # B4: Validate volume
                            vol_result = validate_volume(vol_val)
                            if vol_result["warnings"]:
                                logger.warning(
                                    f"[{exchange.name}] Volume warning for {symbol}: {vol_result['warnings']}"
                                )
                            vol_val = vol_result["volume"]

                            record_dt = datetime.fromtimestamp(
                                ts / 1000, tz=timezone.utc
                            )

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
                                logger.debug(
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
                                    interval="5m",
                                    resolve_required=False,
                                )
                                session.add(new_record)

                        # Update Daily Record using the last candle's close
                        if timestamps and closes:
                            last_ts = timestamps[-1]
                            last_close = closes[-1]
                            record_dt = datetime.fromtimestamp(
                                last_ts / 1000, tz=timezone.utc
                            )
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

                            # Get aggregated values from all candles for daily update
                            valid_opens = [o for o in opens if o is not None]
                            valid_highs = [h for h in highs if h is not None]
                            valid_lows = [l for l in lows if l is not None]
                            valid_closes = [c for c in closes if c is not None]

                            daily_open = valid_opens[0] if valid_opens else None
                            daily_high = max(valid_highs) if valid_highs else None
                            daily_low = min(valid_lows) if valid_lows else None
                            daily_close = valid_closes[-1] if valid_closes else None

                            if not daily_record:
                                logger.debug(
                                    f"Creating new daily record for {symbol} at {daily_dt}"
                                )
                                daily_record = PriceHistoryDaily(
                                    instrument_id=instrument_id,
                                    datetime=daily_dt,
                                    open=daily_open,
                                    high=daily_high,
                                    low=daily_low,
                                    close=daily_close,
                                    volume=0,
                                    resolve_required=False,
                                )
                                session.add(daily_record)

                            # Update Daily Record values
                            daily_record.resolve_required = False
                            if daily_close is not None:
                                daily_record.close = daily_close

                            if daily_record.open is None and daily_open is not None:
                                daily_record.open = daily_open

                            if daily_high is not None and (
                                daily_record.high is None
                                or daily_high > daily_record.high
                            ):
                                daily_record.high = daily_high

                            if daily_low is not None and (
                                daily_record.low is None or daily_low < daily_record.low
                            ):
                                daily_record.low = daily_low

                            # Recalculate daily volume from intraday records
                            day_start = daily_dt
                            day_end = day_start + timedelta(days=1)

                            await session.flush()

                            vol_stmt = select(
                                func.sum(PriceHistoryIntraday.volume)
                            ).where(
                                PriceHistoryIntraday.instrument_id == instrument_id,
                                PriceHistoryIntraday.datetime >= day_start,
                                PriceHistoryIntraday.datetime < day_end,
                            )
                            vol_result = await session.execute(vol_stmt)
                            total_vol = vol_result.scalar() or 0

                            daily_record.volume = total_vol
                            session.add(daily_record)

                    await session.commit()

                    # Update last save time in Redis
                    if self.redis:
                        try:
                            await self.redis.set(
                                f"last_save_time:{interval_minutes}m", str(align_to_ts)
                            )
                            logger.info(
                                f"Updated last_save_time:{interval_minutes}m to {align_to_ts}"
                            )
                        except Exception as e:
                            logger.error(f"Error updating last_save_time in Redis: {e}")

                except Exception as e:
                    logger.error(
                        f"Error saving data for {exchange.name}: {e}", exc_info=True
                    )
                    await session.rollback()

        except Exception as e:
            logger.error(
                f"Error in update_records_for_interval for {exchange.name}: {e}",
                exc_info=True,
            )

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
                Instrument.exchange_id == exchange_id,
            )
            result = await session.execute(stmt)
            instruments = result.scalars().all()

            for inst in instruments:
                self.symbol_map_cache[inst.symbol] = (
                    inst.id,
                    inst.should_record_data,
                    inst.exchange_id,
                )
                if inst.should_record_data:
                    mapping[inst.symbol] = inst.id

        return mapping

    # ============================================================
    # END-OF-DAY TRANSFER: Redis 5m data -> TimescaleDB
    # ============================================================

    async def run_end_of_day_save(self, exchange: Exchange) -> None:
        """
        Run end-of-day save for an exchange.
        Waits for market close + buffer, then transfers Redis 5m data to TimescaleDB.
        """
        exchange_name = exchange.name
        logger.info(f"Starting end-of-day save monitor for {exchange_name}")

        tz = pytz.timezone(exchange.timezone)
        buffer_minutes = 15  # Wait 15 minutes after market close

        try:
            while not self._stop_flags.get(exchange_name, False):
                current_time_ms = int(time.time() * 1000)

                # Update exchange timestamps for current date
                current_date = datetime.now(tz).date()
                exchange.update_timestamps_for_date(current_date)

                # Check if we're past market close + buffer
                save_trigger_time = exchange.end_time + (buffer_minutes * 60 * 1000)

                if current_time_ms >= save_trigger_time:
                    # Check if we already saved for today
                    save_key = f"eod_saved:{exchange_name}:{current_date.isoformat()}"
                    if self.redis:
                        already_saved = await self.redis.get(save_key)
                        if already_saved:
                            # Already saved for today, wait for tomorrow
                            next_date = current_date + timedelta(days=1)
                            exchange.update_timestamps_for_date(next_date)
                            wait_seconds = (
                                exchange.end_time
                                + (buffer_minutes * 60 * 1000)
                                - current_time_ms
                            ) / 1000
                            wait_seconds = max(
                                60.0, min(wait_seconds, 3600.0)
                            )  # At least 1 min, at most 1 hour
                            await asyncio.sleep(wait_seconds)
                            continue

                    # Perform end-of-day save
                    logger.info(
                        f"🌙 Starting end-of-day save for {exchange_name} ({current_date})"
                    )
                    await self.transfer_redis_to_db(exchange, current_date)

                    # Mark as saved
                    if self.redis:
                        await self.redis.set(
                            save_key, "1", ex=24 * 60 * 60
                        )  # Expire in 24 hours

                    logger.info(f"✅ End-of-day save completed for {exchange_name}")

                    # Wait for next day
                    await asyncio.sleep(3600)  # Check again in 1 hour
                else:
                    # Market still open or in pre-close period, wait
                    wait_ms = save_trigger_time - current_time_ms
                    wait_seconds = min(
                        wait_ms / 1000, 300
                    )  # Check at least every 5 minutes
                    await asyncio.sleep(wait_seconds)

        except asyncio.CancelledError:
            logger.info(f"End-of-day save cancelled for {exchange_name}")
        except Exception as e:
            logger.error(
                f"Error in end-of-day save for {exchange_name}: {e}", exc_info=True
            )

    async def transfer_redis_to_db(
        self, exchange: Exchange, trade_date: DateType
    ) -> int:
        """
        Transfer all Redis 5m candle data to TimescaleDB for a given trading day.

        Args:
            exchange: The exchange to transfer data for
            trade_date: The trading date to transfer

        Returns:
            Number of candles transferred
        """
        logger.info(
            f"📦 Transferring Redis data to TimescaleDB for {exchange.name} on {trade_date}"
        )

        tz = pytz.timezone(exchange.timezone)

        # Calculate time range for the trading day
        if exchange.market_open_time and exchange.market_close_time:
            day_start_local = tz.localize(
                datetime.combine(trade_date, exchange.market_open_time)
            )
            day_end_local = tz.localize(
                datetime.combine(trade_date, exchange.market_close_time)
            )
        else:
            day_start_local = tz.localize(
                datetime.combine(trade_date, datetime.strptime("09:00", "%H:%M").time())
            )
            day_end_local = tz.localize(
                datetime.combine(trade_date, datetime.strptime("16:00", "%H:%M").time())
            )

        from_ts = int(day_start_local.timestamp() * 1000)
        to_ts = int(day_end_local.timestamp() * 1000)

        total_transferred = 0

        # Get recordable symbols from Redis
        recordable_symbols = await self.redis_timeseries.get_recordable_symbols()
        if not recordable_symbols:
            logger.warning(f"No recordable symbols found in Redis for {exchange.name}")
            return 0

        async for session in get_db_session():
            try:
                # Get symbol -> instrument_id mapping (only for this exchange)
                symbol_map = await self._get_symbol_to_instrument_mapping(
                    session, recordable_symbols, exchange.id
                )

                for symbol, instrument_id in symbol_map.items():
                    try:
                        # Fetch 5m candles from Redis
                        candles = await self.redis_timeseries.get_5m_candles(
                            symbol=symbol,
                            from_ts=from_ts,
                            to_ts=to_ts,
                        )

                        if not candles or not candles.get("timestamp"):
                            continue

                        timestamps = candles["timestamp"]
                        opens = candles["open"]
                        highs = candles["high"]
                        lows = candles["low"]
                        closes = candles["close"]
                        volumes = candles["volume"]

                        candles_inserted = 0

                        for i, ts in enumerate(timestamps):
                            candle_dt = datetime.fromtimestamp(
                                ts / 1000, tz=timezone.utc
                            )

                            open_val = opens[i]
                            high_val = highs[i]
                            low_val = lows[i]
                            close_val = closes[i]
                            vol_val = int(volumes[i]) if volumes[i] else 0

                            if open_val is None:
                                continue

                            # Validate OHLC
                            ohlc_result = validate_ohlc(
                                open_val,
                                high_val,
                                low_val,
                                close_val,
                                vol_val,
                                fix=True,
                            )
                            if ohlc_result.get("fixed"):
                                open_val = ohlc_result["data"]["open"]
                                high_val = ohlc_result["data"]["high"]
                                low_val = ohlc_result["data"]["low"]
                                close_val = ohlc_result["data"]["close"]

                            # Check if record already exists
                            existing = await session.execute(
                                select(PriceHistoryIntraday).where(
                                    PriceHistoryIntraday.instrument_id == instrument_id,
                                    PriceHistoryIntraday.datetime == candle_dt,
                                )
                            )
                            existing_record = existing.scalar_one_or_none()

                            if existing_record:
                                # Update existing record
                                existing_record.open = open_val
                                existing_record.high = high_val
                                existing_record.low = low_val
                                existing_record.close = close_val
                                existing_record.volume = vol_val
                                existing_record.resolve_required = False
                            else:
                                # Insert new record
                                new_record = PriceHistoryIntraday(
                                    instrument_id=instrument_id,
                                    datetime=candle_dt,
                                    open=open_val,
                                    high=high_val,
                                    low=low_val,
                                    close=close_val,
                                    volume=vol_val,
                                    interval="5m",
                                    resolve_required=False,
                                )
                                session.add(new_record)

                            candles_inserted += 1

                        if candles_inserted > 0:
                            total_transferred += candles_inserted
                            logger.debug(
                                f"Transferred {candles_inserted} candles for {symbol}"
                            )

                    except Exception as e:
                        logger.error(f"Error transferring {symbol}: {e}")
                        continue

                # Update daily records from intraday data
                await self._update_daily_records_from_intraday(
                    session, exchange, trade_date, symbol_map
                )

                await session.commit()
                logger.info(
                    f"📦 Transferred {total_transferred} total candles for {exchange.name}"
                )

            except Exception as e:
                logger.error(f"Error in transfer_redis_to_db: {e}", exc_info=True)
                await session.rollback()
                raise

        return total_transferred

    async def _update_daily_records_from_intraday(
        self,
        session: AsyncSession,
        exchange: Exchange,
        trade_date: DateType,
        symbol_map: Dict[str, int],
    ) -> None:
        """
        Update or create daily records by aggregating intraday data.
        """
        tz = pytz.timezone(exchange.timezone)

        # Calculate the daily record datetime (typically market open time)
        if exchange.market_open_time:
            daily_dt_local = tz.localize(
                datetime.combine(trade_date, exchange.market_open_time)
            )
        else:
            daily_dt_local = tz.localize(
                datetime.combine(trade_date, datetime.strptime("09:00", "%H:%M").time())
            )

        daily_dt_utc = daily_dt_local.astimezone(timezone.utc)
        day_end_utc = daily_dt_utc + timedelta(days=1)

        for symbol, instrument_id in symbol_map.items():
            try:
                # Aggregate intraday data
                agg_stmt = select(
                    func.min(PriceHistoryIntraday.open).label("first_open"),
                    func.max(PriceHistoryIntraday.high).label("high"),
                    func.min(PriceHistoryIntraday.low).label("low"),
                    func.sum(PriceHistoryIntraday.volume).label("volume"),
                ).where(
                    PriceHistoryIntraday.instrument_id == instrument_id,
                    PriceHistoryIntraday.datetime >= daily_dt_utc,
                    PriceHistoryIntraday.datetime < day_end_utc,
                )
                agg_result = await session.execute(agg_stmt)
                agg_row = agg_result.one_or_none()

                if not agg_row or agg_row.high is None:
                    continue

                # Get open (first candle)
                open_stmt = (
                    select(PriceHistoryIntraday.open)
                    .where(
                        PriceHistoryIntraday.instrument_id == instrument_id,
                        PriceHistoryIntraday.datetime >= daily_dt_utc,
                        PriceHistoryIntraday.datetime < day_end_utc,
                        PriceHistoryIntraday.open.isnot(None),
                    )
                    .order_by(PriceHistoryIntraday.datetime.asc())
                    .limit(1)
                )
                open_val = (await session.execute(open_stmt)).scalar_one_or_none()

                # Get close (last candle)
                close_stmt = (
                    select(PriceHistoryIntraday.close)
                    .where(
                        PriceHistoryIntraday.instrument_id == instrument_id,
                        PriceHistoryIntraday.datetime >= daily_dt_utc,
                        PriceHistoryIntraday.datetime < day_end_utc,
                        PriceHistoryIntraday.close.isnot(None),
                    )
                    .order_by(PriceHistoryIntraday.datetime.desc())
                    .limit(1)
                )
                close_val = (await session.execute(close_stmt)).scalar_one_or_none()

                # Find or create daily record
                daily_stmt = select(PriceHistoryDaily).where(
                    PriceHistoryDaily.instrument_id == instrument_id,
                    PriceHistoryDaily.datetime == daily_dt_utc,
                )
                daily_result = await session.execute(daily_stmt)
                daily_record = daily_result.scalar_one_or_none()

                if daily_record:
                    daily_record.open = open_val
                    daily_record.high = agg_row.high
                    daily_record.low = agg_row.low
                    daily_record.close = close_val
                    daily_record.volume = int(agg_row.volume) if agg_row.volume else 0
                    daily_record.resolve_required = False
                else:
                    daily_record = PriceHistoryDaily(
                        instrument_id=instrument_id,
                        datetime=daily_dt_utc,
                        open=open_val,
                        high=agg_row.high,
                        low=agg_row.low,
                        close=close_val,
                        volume=int(agg_row.volume) if agg_row.volume else 0,
                        resolve_required=False,
                    )
                    session.add(daily_record)

            except Exception as e:
                logger.error(f"Error updating daily record for {symbol}: {e}")
                continue

    async def start_end_of_day_monitors(self) -> None:
        """Start end-of-day save monitors for all exchanges."""
        if not self.exchanges:
            logger.warning("No exchanges registered for end-of-day monitoring")
            return

        for exchange in self.exchanges:
            task = asyncio.create_task(self.run_end_of_day_save(exchange))
            self._running_tasks[f"{exchange.name}_eod"] = task

        logger.info(
            f"Started end-of-day monitors for {len(self.exchanges)} exchange(s)"
        )
