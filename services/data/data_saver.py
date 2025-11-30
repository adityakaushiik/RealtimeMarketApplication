import asyncio
import time
from datetime import datetime
from typing import List, Optional, Dict
from sqlalchemy import select
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
    DataSaver handles periodic saves of market data from Redis TimeSeries to the database.

    Features:
    - Saves 5-minute interval OHLCV data to price_history_intraday table
    - At end of trading day, aggregates intraday data to price_history_daily table
    - Supports multiple exchanges with different trading hours
    """

    def __init__(self):
        self.redis_timeseries = get_redis_timeseries()
        self.exchanges: List[ExchangeData] = []
        self._running_tasks: Dict[str, asyncio.Task] = {}
        self._stop_flags: Dict[str, bool] = {}
        self.symbol_map_cache: Dict[str, int] = {}  # Cache for symbol -> instrument_id

    def add_exchange(self, exchange_data: ExchangeData) -> None:
        """Add an exchange to monitor for data collection."""
        self.exchanges.append(exchange_data)
        logger.info(f"Added exchange: {exchange_data.exchange_name}")

    async def save_to_intraday_table(
        self,
        exchange_id: Optional[int] = None,
        instrument_ids: Optional[List[int]] = None,
        interval_minutes: int = 5
    ) -> int:
        """
        Save the stock price data to the intraday table in the database.

        Args:
            exchange_id: Optional filter by exchange ID
            instrument_ids: Optional list of specific instrument IDs to save
            interval_minutes: Interval in minutes for OHLCV aggregation (default: 5)

        Returns:
            Number of records saved
        """
        keys = await self.redis_timeseries.get_all_keys()

        if not keys:
            logger.warning("No keys found in Redis TimeSeries")
            return 0

        logger.info(f"Fetching OHLCV data for {len(keys)} symbols with {interval_minutes}-minute interval")
        data = await self.redis_timeseries.get_all_ohlcv_last_5m(keys, interval_minutes=interval_minutes)
        print(data)

        # Discard any null values
        valid_data = {k: v for k, v in data.items() if v is not None}

        logger.info(f"Found {len(valid_data)} symbols with valid OHLCV data (filtered out {len(data) - len(valid_data)} null values)")

        # Log sample data for debugging
        if valid_data:
            sample_symbol = list(valid_data.keys())[0]
            sample_data = valid_data[sample_symbol]
            logger.info(f"Sample OHLCV data - Symbol: {sample_symbol}, Data: {sample_data}")
            logger.info(f"Data type: {type(sample_data)}, Keys: {sample_data.keys() if isinstance(sample_data, dict) else 'N/A'}")

        if not valid_data:
            logger.warning(f"No valid OHLCV data found for {len(keys)} keys. (Providers might be silent or data is stale)")
            return 0

        saved_count = 0
        async for session in get_db_session():
            try:
                # Get instrument mappings from symbols
                symbol_to_instrument = await self._get_symbol_to_instrument_mapping(
                    session,
                    list(valid_data.keys()),
                    exchange_id
                )

                records_to_save = []
                daily_updates = []  # Track instruments for daily updates

                # Tracking counters for debugging
                not_found_count = 0
                filtered_count = 0
                incomplete_count = 0

                for symbol, ohlcv in valid_data.items():
                    # Get instrument_id from symbol
                    instrument_id = symbol_to_instrument.get(symbol)

                    if instrument_id is None:
                        not_found_count += 1
                        logger.debug(f"Instrument not found for symbol: {symbol}")
                        continue

                    # Filter by instrument_ids if provided
                    if instrument_ids and instrument_id not in instrument_ids:
                        filtered_count += 1
                        continue

                    # Check if all OHLC values are present
                    if not all(ohlcv.get(k) is not None for k in ["open", "high", "low", "close"]):
                        incomplete_count += 1
                        missing = [k for k in ["open", "high", "low", "close"] if ohlcv.get(k) is None]
                        logger.warning(f"Incomplete OHLC data for {symbol}: missing {missing}, data: {ohlcv}")
                        continue

                    # Create intraday record
                    intraday_record = PriceHistoryIntraday(
                        instrument_id=instrument_id,
                        datetime=datetime.fromtimestamp(int(ohlcv["ts"]) / 1000),
                        open=ohlcv["open"],
                        high=ohlcv["high"],
                        low=ohlcv["low"],
                        close=ohlcv["close"],
                        volume=int(ohlcv.get("volume", 0)),
                        interval="5m",
                        price_not_found=False
                    )
                    records_to_save.append(intraday_record)

                    # Track for daily update
                    daily_updates.append((instrument_id, ohlcv))

                if records_to_save:
                    session.add_all(records_to_save)

                    # Get today's date timestamp for daily records
                    date_timestamp = self._get_start_of_day_timestamp(int(time.time() * 1000))

                    # Batch update/insert daily records
                    await self._batch_upsert_daily_records(
                        session,
                        daily_updates,
                        date_timestamp
                    )

                    await session.commit()
                    saved_count = len(records_to_save)
                    logger.info(f"Saved {saved_count} intraday records and updated {len(daily_updates)} daily records")
                else:
                    logger.warning(f"No valid records to save. Rejection reasons: "
                                 f"not_found={not_found_count}, filtered={filtered_count}, incomplete={incomplete_count}")
                    logger.warning(f"Total valid_data received: {len(valid_data)}, Instruments in DB: {len(symbol_to_instrument)}")

            except Exception as e:
                logger.error(f"Error saving to intraday table: {e}", exc_info=True)
                await session.rollback()
                raise

        return saved_count

    async def _get_symbol_to_instrument_mapping(
        self,
        session: AsyncSession,
        symbols: List[str],
        exchange_id: Optional[int] = None
    ) -> Dict[str, int]:
        """
        Get mapping from symbol to instrument_id with caching.

        Args:
            session: Database session
            symbols: List of symbols
            exchange_id: Optional exchange ID filter

        Returns:
            Dictionary mapping symbol to instrument_id
        """
        mapping = {}
        missing_symbols = []

        # Check cache first
        for symbol in symbols:
            if symbol in self.symbol_map_cache:
                mapping[symbol] = self.symbol_map_cache[symbol]
            else:
                missing_symbols.append(symbol)

        if not missing_symbols:
            return mapping

        # Fetch missing symbols from DB
        query = select(Instrument.symbol, Instrument.id).where(
            Instrument.symbol.in_(missing_symbols),
            Instrument.blacklisted == False,
            Instrument.delisted == False
        )

        if exchange_id:
            query = query.where(Instrument.exchange_id == exchange_id)

        result = await session.execute(query)
        rows = result.all()

        # Update cache and mapping
        for row in rows:
            self.symbol_map_cache[row.symbol] = row.id
            mapping[row.symbol] = row.id

        return mapping

    async def _batch_upsert_daily_records(
        self,
        session: AsyncSession,
        daily_updates: List[tuple],
        date_timestamp: int
    ) -> None:
        """
        Batch insert or update daily records with incremental OHLCV data.
        Optimized to avoid N+1 queries.
        """
        if not daily_updates:
            return

        instrument_ids = list(set(update[0] for update in daily_updates))

        # Fetch existing records in one query
        date_dt = datetime.fromtimestamp(date_timestamp / 1000)
        query = select(PriceHistoryDaily).where(
            PriceHistoryDaily.instrument_id.in_(instrument_ids),
            PriceHistoryDaily.datetime == date_dt
        )
        result = await session.execute(query)
        existing_records = {row.instrument_id: row for row in result.scalars().all()}

        for instrument_id, ohlcv in daily_updates:
            existing_daily = existing_records.get(instrument_id)

            if existing_daily:
                # Update existing record with incremental aggregation
                if ohlcv["high"] is not None:
                    existing_daily.high = max(existing_daily.high or float('-inf'), ohlcv["high"])
                if ohlcv["low"] is not None:
                    existing_daily.low = min(existing_daily.low or float('inf'), ohlcv["low"])
                if ohlcv["close"] is not None:
                    existing_daily.close = ohlcv["close"]
                if ohlcv["volume"] is not None:
                    existing_daily.volume = (existing_daily.volume or 0) + int(ohlcv["volume"])
            else:
                # Create new daily record
                new_daily = PriceHistoryDaily(
                    instrument_id=instrument_id,
                    datetime=datetime.fromtimestamp(date_timestamp / 1000),
                    open=ohlcv["open"],
                    high=ohlcv["high"],
                    low=ohlcv["low"],
                    close=ohlcv["close"],
                    volume=int(ohlcv.get("volume", 0)),
                    price_not_found=False
                )
                session.add(new_daily)
                # Add to map to handle potential duplicates in the same batch (though unlikely for 5m interval)
                existing_records[instrument_id] = new_daily

        logger.debug(f"Processed batch update for {len(daily_updates)} daily records")

    async def run_periodic_save(self, exchange_data: ExchangeData, interval_minutes: Optional[int] = None) -> None:
        """
        Run periodic saves for a specific exchange from start_time to end_time.
        Saves data every 5 minutes and aggregates to daily at the end.

        Args:
            exchange_data: Exchange configuration with start_time and end_time
            interval_minutes: Optional interval in minutes (overrides exchange_data.interval_minutes)
        """
        exchange_name = exchange_data.exchange_name

        # Use provided interval_minutes or fall back to exchange_data.interval_minutes
        interval = interval_minutes if interval_minutes is not None else exchange_data.interval_minutes

        logger.info(f"Starting periodic save for {exchange_name}")
        logger.info(f"Start: {datetime.fromtimestamp(exchange_data.start_time/1000).strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"End: {datetime.fromtimestamp(exchange_data.end_time/1000).strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"Interval: {interval} minutes")

        interval_ms = interval * 60 * 1000
        current_time_ms = int(time.time() * 1000)

        # If start_time is in the future, wait
        if current_time_ms < exchange_data.start_time:
            wait_ms = exchange_data.start_time - current_time_ms
            logger.info(f"Waiting {wait_ms/1000:.0f} seconds until market opens...")
            await asyncio.sleep(wait_ms / 1000)

        try:
            while not self._stop_flags.get(exchange_name, False):
                current_time_ms = int(time.time() * 1000)

                # Check if we've reached end_time
                if current_time_ms >= exchange_data.end_time:
                    logger.info(f"Reached end_time for {exchange_name}, performing final save")

                    # Final intraday save (which also updates daily record)
                    await self.save_to_intraday_table(
                        exchange_id=exchange_data.exchange_id,
                        interval_minutes=interval
                    )

                    logger.info(f"Completed periodic save for {exchange_name}")
                    logger.info(f"Daily records have been incrementally updated throughout the day")
                    break

                # Perform periodic save
                try:
                    saved_count = await self.save_to_intraday_table(
                        exchange_id=exchange_data.exchange_id,
                        interval_minutes=interval
                    )
                    logger.info(f"[{exchange_name}] Periodic save completed: {saved_count} records")
                except Exception as e:
                    logger.error(f"[{exchange_name}] Error during periodic save: {e}", exc_info=True)

                # Wait for next interval
                await asyncio.sleep(interval_ms / 1000)

        except asyncio.CancelledError:
            logger.info(f"Periodic save cancelled for {exchange_name}")
            raise
        except Exception as e:
            logger.error(f"Fatal error in periodic save for {exchange_name}: {e}", exc_info=True)
            raise

    def _get_start_of_day_timestamp(self, timestamp_ms: int) -> int:
        """Get the start of day timestamp (midnight) for a given timestamp."""
        dt = datetime.fromtimestamp(timestamp_ms / 1000)
        start_of_day = dt.replace(hour=0, minute=0, second=0, microsecond=0)
        return int(start_of_day.timestamp() * 1000)

    async def start_all_exchanges(self, interval_minutes: Optional[int] = None) -> None:
        """
        Start periodic saves for all registered exchanges.

        Args:
            interval_minutes: Optional interval in minutes for all exchanges (overrides exchange_data.interval_minutes)
        """
        if not self.exchanges:
            logger.warning("No exchanges registered")
            return

        for exchange_data in self.exchanges:
            task = asyncio.create_task(self.run_periodic_save(exchange_data, interval_minutes=interval_minutes))
            self._running_tasks[exchange_data.exchange_name] = task

        logger.info(f"Started periodic saves for {len(self.exchanges)} exchange(s)")

    async def stop_all_exchanges(self) -> None:
        """Stop all running periodic saves."""
        for exchange_name in self._running_tasks:
            self._stop_flags[exchange_name] = True

        # Wait for all tasks to complete
        if self._running_tasks:
            await asyncio.gather(*self._running_tasks.values(), return_exceptions=True)

        self._running_tasks.clear()
        self._stop_flags.clear()
        logger.info("Stopped all periodic saves")

    async def wait_for_completion(self) -> None:
        """Wait for all periodic save tasks to complete."""
        if self._running_tasks:
            await asyncio.gather(*self._running_tasks.values(), return_exceptions=True)
            logger.info("All periodic save tasks completed")
