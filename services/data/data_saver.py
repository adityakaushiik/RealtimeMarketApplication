import asyncio
import time
from datetime import datetime, timedelta
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
            task = asyncio.create_task(self.run_periodic_save(exchange_data, interval_minutes))
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

    async def run_periodic_save(self, exchange_data: ExchangeData, interval_minutes: Optional[int] = None) -> None:
        """
        Run periodic updates for a specific exchange.
        """
        exchange_name = exchange_data.exchange_name
        interval = interval_minutes if interval_minutes is not None else exchange_data.interval_minutes
        interval_ms = interval * 60 * 1000

        logger.info(f"Starting periodic update for {exchange_name} (Interval: {interval}m)")

        try:
            while not self._stop_flags.get(exchange_name, False):
                current_time_ms = int(time.time() * 1000)
                
                # Check if market is open or if we should do a final save
                if current_time_ms > exchange_data.end_time + interval_ms:
                     logger.info(f"Market closed for {exchange_name}. Stopping updates.")
                     break

                if current_time_ms < exchange_data.start_time:
                    wait_ms = exchange_data.start_time - current_time_ms
                    logger.info(f"Waiting {wait_ms/1000:.0f}s for market open...")
                    await asyncio.sleep(wait_ms / 1000)
                    continue

                # Calculate alignment to the next interval
                # We want to run slightly after the interval closes to ensure data is available
                next_interval_ts = ((current_time_ms // interval_ms) + 1) * interval_ms
                wait_ms = next_interval_ts - current_time_ms + 1000 # Add 1s buffer
                
                await asyncio.sleep(wait_ms / 1000)

                if self._stop_flags.get(exchange_name, False):
                    break

                await self.update_records_for_interval(exchange_data, interval)

        except asyncio.CancelledError:
            logger.info(f"Periodic update cancelled for {exchange_name}")
        except Exception as e:
            logger.error(f"Fatal error in periodic update for {exchange_name}: {e}", exc_info=True)

    async def update_records_for_interval(self, exchange_data: ExchangeData, interval_minutes: int) -> None:
        """Fetch data from Redis and update existing DB records."""
        try:
            keys = await self.redis_timeseries.get_all_keys()
            if not keys:
                return

            # Fetch OHLCV data for the last interval
            # We use bucket_minutes=interval_minutes to get a single candle for the interval
            tasks = [
                self.redis_timeseries.get_ohlcv_window(
                    key, 
                    window_minutes=interval_minutes, 
                    bucket_minutes=interval_minutes
                )
                for key in keys
            ]
            results = await asyncio.gather(*tasks)
            
            valid_data = {}
            for key, result in zip(keys, results):
                # Check if we have valid data (lists are not empty and have values)
                if result and result.get("timestamp") and result["timestamp"][0]:
                     valid_data[key] = result

            if not valid_data:
                return

            async for session in get_db_session():
                try:
                    symbol_map = await self._get_symbol_to_instrument_mapping(session, list(valid_data.keys()), exchange_data.exchange_id)
                    
                    for symbol, data in valid_data.items():
                        instrument_id = symbol_map.get(symbol)
                        if not instrument_id:
                            continue

                        # Extract scalar values from lists (taking the last/only bucket)
                        idx = -1
                        ts = data["timestamp"][idx]
                        open_val = data["open"][idx]
                        high_val = data["high"][idx]
                        low_val = data["low"][idx]
                        close_val = data["close"][idx]
                        vol_val = int(data["volume"][idx]) if data["volume"][idx] is not None else 0

                        if open_val is None: # Skip if no data in this bucket
                            continue

                        record_dt = datetime.fromtimestamp(ts / 1000)

                        # Update Intraday Record
                        stmt = (
                            update(PriceHistoryIntraday)
                            .where(
                                PriceHistoryIntraday.instrument_id == instrument_id,
                                PriceHistoryIntraday.datetime == record_dt
                            )
                            .values(
                                open=open_val,
                                high=high_val,
                                low=low_val,
                                close=close_val,
                                volume=vol_val,
                                price_not_found=False
                            )
                        )
                        await session.execute(stmt)

                        # Update Daily Record (Incremental update logic could be complex, 
                        # but for simplicity and correctness with pre-created records, 
                        # we might want to fetch and update, or just update high/low/close/vol)
                        
                        # For daily, we need to aggregate. Since we are updating incrementally,
                        # we can use SQL expressions to update high/low/vol.
                        # However, 'close' should be the latest close. 'open' should be set if not set?
                        # Since records are pre-created with nulls, we can handle the first update vs subsequent.
                        
                        # Simplified Daily Update:
                        # We update High if current high is higher, Low if lower, etc.
                        # But since we don't know if it's the first update, we need to handle NULLs.
                        # COALESCE is useful here.
                        
                        # Note: This assumes the daily record exists for the day of record_dt
                        daily_dt = record_dt.replace(hour=0, minute=0, second=0, microsecond=0) # Or use market open time?
                        # DataCreationService uses market_open_time for daily record datetime.
                        # We should match that. But here we might just use the date.
                        # Let's assume we query by date(datetime) cast or range.
                        # But simpler: DataCreationService used `start_dt` which is market open time.
                        
                        # Let's try to find the daily record for this instrument and date
                        # We can't easily know the exact datetime used in creation without querying or assuming.
                        # But we know it's for the same day.
                        
                        # Actually, let's just update the daily record that matches the date.
                        # We can use a subquery or just fetch it first. Fetching is safer.
                        
                        daily_stmt = select(PriceHistoryDaily).where(
                            PriceHistoryDaily.instrument_id == instrument_id,
                            # We need to match the date part. 
                            # In SQLAlchemy async, func.date(PriceHistoryDaily.datetime) == record_dt.date()
                            # But let's just use a time range for the day to be index-friendly if possible.
                            PriceHistoryDaily.datetime >= daily_dt,
                            PriceHistoryDaily.datetime < daily_dt + timedelta(days=1)
                        )
                        daily_result = await session.execute(daily_stmt)
                        daily_record = daily_result.scalar_one_or_none()

                        if daily_record:
                            daily_record.price_not_found = False
                            daily_record.close = close_val
                            daily_record.volume = (daily_record.volume or 0) + vol_val
                            
                            if daily_record.open is None:
                                daily_record.open = open_val
                            
                            if daily_record.high is None or high_val > daily_record.high:
                                daily_record.high = high_val
                                
                            if daily_record.low is None or low_val < daily_record.low:
                                daily_record.low = low_val
                            
                            session.add(daily_record)

                    await session.commit()
                    logger.info(f"Updated records for {len(valid_data)} symbols at {datetime.now()}")

                except Exception as e:
                    logger.error(f"Error updating records: {e}")
                    await session.rollback()

        except Exception as e:
            logger.error(f"Error in update_records_for_interval: {e}")

    async def _get_symbol_to_instrument_mapping(self, session: AsyncSession, symbols: List[str], exchange_id: int) -> Dict[str, int]:
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
                Instrument.symbol.in_(missing),
                Instrument.exchange_id == exchange_id
            )
            result = await session.execute(stmt)
            for row in result.all():
                self.symbol_map_cache[row.symbol] = row.id
                mapping[row.symbol] = row.id
        
        return mapping

