import datetime
from typing import Dict, Optional, List, Set
import pytz
from sqlalchemy import select
from sqlalchemy.orm import joinedload

from config.database_config import get_db_session
from config.logger import logger
from models.exchange import Exchange


class MarketHoursManager:
    _instance = None
    
    def __init__(self):
        self.exchanges: Dict[int, Exchange] = {}
        self.last_updated = datetime.datetime.min
        self._exchange_open_status_cache: Dict[int, bool] = {}
        # Cache specific holidays for quick lookup:  exchange_id -> set of dates
        self.holidays: Dict[int, Set[datetime.date]] = {}
        # Cache holiday details: exchange_id -> {date: List[dict]}
        self.holiday_details: Dict[int, Dict[datetime.date, List[dict]]] = {}

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
                # Eager load holidays
                result = await session.execute(
                    select(Exchange).options(joinedload(Exchange.holidays))
                )
                exchanges = result.scalars().unique().all()
                
                # Update cache
                today = datetime.datetime.now().date()
                for exchange in exchanges:
                    # Ensure timestamps are updated for today
                    exchange.update_timestamps_for_date(today)
                    self.exchanges[exchange.id] = exchange
                    
                    # Cache holidays
                    holiday_map = {}
                    for h in exchange.holidays:
                        if h.date not in holiday_map:
                            holiday_map[h.date] = []
                        
                        holiday_map[h.date].append({
                            "id": h.id,
                            "exchange_id": h.exchange_id,
                            "date": h.date,
                            "description": h.description,
                            "is_closed": h.is_closed,
                            "open_time": h.open_time,
                            "close_time": h.close_time
                        })

                    self.holiday_details[exchange.id] = holiday_map
                    
                    self.holidays[exchange.id] = {
                        h.date for h in exchange.holidays if h.is_closed
                    }
                
                logger.info(f"MarketHoursManager: Loaded {len(exchanges)} exchanges and their holidays.")
                self.last_updated = datetime.datetime.now()
                break # Close session after one use from generator
        except Exception as e:
            logger.error(f"Error loading exchanges in MarketHoursManager: {e}")

    def is_holiday(self, exchange_id: int, date_obj: Optional[datetime.date] = None) -> bool:
        """Check if the given date is a holiday for the exchange."""
        if not exchange_id:
            return False
            
        if date_obj is None:
            # Default to today in exchange timezone? Or just UTC today?
            # Ideally provided, but if not, use UTC today
            date_obj = datetime.datetime.now(datetime.timezone.utc).date()
            
        holidays = self.holidays.get(exchange_id)
        if holidays and date_obj in holidays:
            return True
        return False

    def get_holidays_for_date(
        self, exchange_id: int, date_obj: Optional[datetime.date] = None
    ) -> List[dict]:
        """Get the list of holiday details for the given date."""
        if not exchange_id:
            return []

        if date_obj is None:
            date_obj = datetime.datetime.now(datetime.timezone.utc).date()

        details = self.holiday_details.get(exchange_id)
        if details and date_obj in details:
            return details[date_obj]
        return []

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
            
        # Check holiday
        tz = pytz.timezone(exchange.timezone)
        dt = datetime.datetime.fromtimestamp(timestamp_ms / 1000, tz)
        if self.is_holiday(exchange_id, dt.date()):
            return False

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
