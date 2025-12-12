import asyncio
import time
import pytz
from datetime import datetime, timedelta, timezone
from typing import List, Optional, Dict, Any
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from services.redis_timeseries import get_redis_timeseries
from services.data.exchange_data import ExchangeData
from models.instruments import Instrument
from models.price_history_intraday import PriceHistoryIntraday
from models.price_history_daily import PriceHistoryDaily
from config.database_config import get_db_session
from config.logger import logger


class DataSaver:
    """
    DataSaver handles periodic updates of market data from Redis TimeSeries to the database.
    It updates existing records created by DataCreationService.
    """

    def __init__(self):
        self.redis_timeseries = get_redis_timeseries()
        self.exchanges: List[ExchangeData] = []
        self._running_tasks: Dict[str, asyncio.Task] = {}
        self._stop_flags: Dict[str, bool] = {}
        self.symbol_map_cache: Dict[str, int] = {}

    def add_exchange(self, exchange_data: ExchangeData) -> None:
        """Add an exchange to monitor."""
        self.exchanges.append(exchange_data)
        logger.info(f"Added exchange: {exchange_data.exchange_name}")

    async def start_all_exchanges(self, interval_minutes: Optional[int] = None) -> None:
        """Start periodic updates for all exchanges."""
        if not self.exchanges:
            logger.warning("No exchanges registered")
            return

        for exchange_data in self.exchanges:
            task = asyncio.create_task(
                self.run_periodic_save(exchange_data, interval_minutes)
            )
            self._running_tasks[exchange_data.exchange_name] = task

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
        self, exchange_data: ExchangeData, interval_minutes: Optional[int] = None
    ) -> None:
        """
        Run periodic updates for a specific exchange.
        """
        exchange_name = exchange_data.exchange_name
        interval = (
            interval_minutes
            if interval_minutes is not None
            else exchange_data.interval_minutes
        )
        interval_ms = interval * 60 * 1000

        logger.info(
            f"Starting periodic update for {exchange_name} (Interval: {interval}m)"
        )

        tz = pytz.timezone(exchange_data.timezone_str)

        try:
            while not self._stop_flags.get(exchange_name, False):
                current_time_ms = int(time.time() * 1000)
                logger.debug(
                    f"[{exchange_name}] Check: Current={current_time_ms}, Start={exchange_data.start_time}, End={exchange_data.end_time}"
                )

                # Check if market is open or if we should do a final save
                if current_time_ms > exchange_data.end_time + interval_ms:
                    # Market closed for the current configured day.
                    # Check if we need to roll over to tomorrow or if we just need to update to today (if stale).
                    current_date = datetime.now(tz).date()

                    # Update to current date first
                    exchange_data.update_timestamps_for_date(current_date)

                    # Check again
                    if current_time_ms > exchange_data.end_time + interval_ms:
                        # Still closed for today, so move to tomorrow
                        next_date = current_date + timedelta(days=1)
                        exchange_data.update_timestamps_for_date(next_date)
                        logger.info(
                            f"Market closed for {exchange_name}. Waiting for next session on {next_date}..."
                        )
                    else:
                        logger.info(
                            f"Updated schedule for {exchange_name} to today {current_date}"
                        )

                    # Continue to loop to hit the wait block
                    continue

                if current_time_ms < exchange_data.start_time:
                    wait_ms = exchange_data.start_time - current_time_ms
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
                    exchange_data, interval, target_bucket_ts
                )

        except asyncio.CancelledError:
            logger.info(f"Periodic update cancelled for {exchange_name}")
        except Exception as e:
            logger.error(
                f"Fatal error in periodic update for {exchange_name}: {e}",
                exc_info=True,
            )

    async def update_records_for_interval(
        self, exchange_data: ExchangeData, interval_minutes: int, align_to_ts: int
    ) -> None:
        """Fetch data from Redis and update existing DB records."""
        try:
            keys = await self.redis_timeseries.get_all_keys()
            if not keys:
                logger.warning(
                    f"[{exchange_data.exchange_name}] No keys found in Redis"
                )
                return

            logger.info(
                f"[{exchange_data.exchange_name}] Found {len(keys)} keys. Fetching OHLCV aligned to {align_to_ts}..."
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
            for key, result in zip(keys, results):
                # Check if we have valid data
                if result and result.get("ts"):
                    valid_data[key] = result

            if not valid_data:
                logger.warning(
                    f"[{exchange_data.exchange_name}] No valid data found for interval"
                )
                return

            logger.info(
                f"[{exchange_data.exchange_name}] Updating DB for {len(valid_data)} symbols..."
            )

            async for session in get_db_session():
                try:
                    symbol_map = await self._get_symbol_to_instrument_mapping(
                        session, list(valid_data.keys()), exchange_data.exchange_id
                    )

                    for symbol, data in valid_data.items():
                        instrument_id = symbol_map.get(symbol)
                        if not instrument_id:
                            continue

                        # Extract scalar values
                        ts = data["ts"]
                        open_val = data["open"]
                        high_val = data["high"]
                        low_val = data["low"]
                        close_val = data["close"]
                        vol_val = (
                            int(data["volume"]) if data["volume"] is not None else 0
                        )

                        if open_val is None:  # Skip if no data in this bucket
                            continue

                        # Use UTC timezone for record_dt to match DB storage
                        # The timestamp from Redis is the START of the bucket.
                        # For 10:00-10:05 bucket, Redis returns 10:00 timestamp.
                        # This matches our DB record for 10:00.
                        record_dt = datetime.fromtimestamp(ts / 1000, tz=timezone.utc)

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
                                price_not_found=False,
                            )
                        )
                        result = await session.execute(stmt)
                        if result.rowcount == 0:
                            logger.warning(
                                f"No intraday record found to update for {symbol} at {record_dt} (Instrument ID: {instrument_id})"
                            )

                        # Update Daily Record
                        # Daily record timestamp is exactly the market open time for the current session
                        # We use exchange_data.market_open_time which is computed for "today" (current session)
                        # Note: We must use the official market open time, not pre-market start
                        # We need to get the timezone object again or use the one from outer scope if available
                        # But simpler to just use the exchange_data helper which handles it internally if we pass the date
                        # We can derive the date from record_dt (which is UTC) converted to exchange timezone

                        record_dt_local = record_dt.astimezone(
                            pytz.timezone(exchange_data.timezone_str)
                        )

                        daily_dt = datetime.fromtimestamp(
                            exchange_data._compute_timestamp(
                                record_dt_local.date(), exchange_data.market_open_time
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

                        if daily_record:
                            daily_record.price_not_found = False
                            daily_record.close = close_val
                            daily_record.volume = (daily_record.volume or 0) + vol_val

                            if daily_record.open is None:
                                daily_record.open = open_val

                            if (
                                daily_record.high is None
                                or high_val > daily_record.high
                            ):
                                daily_record.high = high_val

                            if daily_record.low is None or low_val < daily_record.low:
                                daily_record.low = low_val

                            session.add(daily_record)
                        else:
                            logger.warning(
                                f"No daily record found for {symbol} at {daily_dt}"
                            )

                    await session.commit()
                    logger.info(
                        f"Updated records for {len(valid_data)} symbols at {datetime.now()}"
                    )

                except Exception as e:
                    logger.error(f"Error updating records: {e}")
                    await session.rollback()

        except Exception as e:
            logger.error(f"Error in update_records_for_interval: {e}")

    async def _get_symbol_to_instrument_mapping(
        self, session: AsyncSession, symbols: List[str], exchange_id: int
    ) -> Dict[str, int]:
        """Get mapping from symbol to instrument_id."""
        mapping = {}
        missing = []

        for s in symbols:
            if s in self.symbol_map_cache:
                mapping[s] = self.symbol_map_cache[s]
            else:
                missing.append(s)

        if missing:
            stmt = select(Instrument.symbol, Instrument.id).where(
                Instrument.symbol.in_(missing), Instrument.exchange_id == exchange_id
            )
            result = await session.execute(stmt)
            for row in result.all():
                self.symbol_map_cache[row.symbol] = row.id
                mapping[row.symbol] = row.id

        return mapping
