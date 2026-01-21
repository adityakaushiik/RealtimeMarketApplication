import datetime
from typing import Dict, Optional, List
import pytz
from sqlalchemy import select

from config.database_config import get_db_session
from config.logger import logger
from models.exchange import Exchange


class MarketHoursManager:
    _instance = None
    
    def __init__(self):
        self.exchanges: Dict[int, Exchange] = {}
        self.last_updated = datetime.datetime.min
        self._exchange_open_status_cache: Dict[int, bool] = {}

    @classmethod
    def get_instance(cls):
        if cls._instance is None:
            cls._instance = MarketHoursManager()
        return cls._instance

    async def initialize(self):
        """Load exchanges from DB"""
        await self.refresh_exchanges()

    async def refresh_exchanges(self):
        try:
            async for session in get_db_session():
                result = await session.execute(select(Exchange))
                exchanges = result.scalars().all()
                
                # Update cache
                today = datetime.datetime.now().date()
                for exchange in exchanges:
                    # Ensure timestamps are updated for today
                    exchange.update_timestamps_for_date(today)
                    self.exchanges[exchange.id] = exchange
                
                logger.info(f"MarketHoursManager: Loaded {len(exchanges)} exchanges.")
                self.last_updated = datetime.datetime.now()
                break # Close session after one use from generator
        except Exception as e:
            logger.error(f"Error loading exchanges in MarketHoursManager: {e}")

    def is_market_open(self, exchange_id: int, timestamp_ms: int) -> bool:
        """
        Check if market is open for the given exchange at the given timestamp.
        Returns True if open, False if closed.
        Returns True if exchange info is missing (fail open).
        """
        if not exchange_id:
            return True

        exchange = self.exchanges.get(exchange_id)
        if not exchange:
            # Exchange not found, assume open to allow data flow (or could fetch)
            return True

        # Check if 24 hours
        if exchange.is_open_24_hours:
            return True

        # Check pre-calculated timestamps (which are UTC)
        # Note: exchange.start_time and end_time are set for "today".
        # If the timestamp is for a different day, this might be inaccurate 
        # but for real-time data ingestion, timestamp is usually "now".
        
        # Buffer: Allow data 5 minutes before open (pre-market activity sometimes comes as ticks)
        # and 15 minutes after close (closing prints).
        buffer_ms = 15 * 60 * 1000 
        
        if timestamp_ms >= (exchange.start_time - buffer_ms) and timestamp_ms <= (exchange.end_time + buffer_ms):
            return True
        
        return False

def get_market_hours_manager() -> MarketHoursManager:
    return MarketHoursManager.get_instance()
