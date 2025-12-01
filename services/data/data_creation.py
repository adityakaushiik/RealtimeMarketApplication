import asyncio
from datetime import datetime, timedelta
from typing import List, Optional
import pytz

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from config.logger import logger
from services.data.exchange_data import ExchangeData
from models.instruments import Instrument
from models.price_history_intraday import PriceHistoryIntraday
from models.price_history_daily import PriceHistoryDaily


class DataCreationService:

    def __init__(self, session: AsyncSession):
        self.exchanges: List[ExchangeData] = []
        self.session: AsyncSession = session

    def add_exchange(self, exchange_data: ExchangeData) -> None:
        """Add an exchange to monitor for data collection."""
        self.exchanges.append(exchange_data)
        logger.info(f"Added exchange: {exchange_data.exchange_name}")

    def list_exchanges(self) -> List[str]:
        """List the names of all exchanges being monitored."""
        return [exchange.exchange_name for exchange in self.exchanges]

    async def start_data_creation(self, offset_days: int = 0) -> None:
        """
        Create Data Records for the future for all monitored exchanges.
        this includes record for intraday and daily data.
        
        Args:
            offset_days: Number of days to offset from today (default: 0)
        """
        # Run tasks sequentially to avoid sharing the same session concurrently
        for exchange in self.exchanges:
            await self._create_data_records_for_exchange(exchange, offset_days)

    async def _create_data_records_for_exchange(self, exchange: ExchangeData, offset_days: int = 0) -> None:
        """Create data records for a specific exchange."""
        try:
            # Calculate target date and times
            tz = pytz.timezone(exchange.timezone_str)
            now = datetime.now(tz)
            target_date = now.date() + timedelta(days=offset_days)
            
            # Calculate start and end times for the target date
            start_dt = tz.localize(datetime.combine(target_date, exchange.market_open_time))
            end_dt = tz.localize(datetime.combine(target_date, exchange.market_close_time))
            
            logger.info(f"Creating data records for {exchange.exchange_name} on {target_date} (Offset: {offset_days})")

            # Fetch active instruments for this exchange
            stmt = select(Instrument).where(
                Instrument.exchange_id == exchange.exchange_id, 
                Instrument.is_active == True
            )
            result = await self.session.execute(stmt)
            instruments = result.scalars().all()
            
            if not instruments:
                logger.warning(f"No active instruments found for {exchange.exchange_name}")
                return

            # Generate 5-minute intervals
            five_minute_datetimes = []
            current_dt = start_dt
            while current_dt <= end_dt:
                five_minute_datetimes.append(current_dt)
                current_dt += timedelta(minutes=exchange.interval_minutes)

            intraday_records = []
            daily_records = []

            for instrument in instruments:
                # Create Daily Record (placeholder)
                daily_record = PriceHistoryDaily(
                    instrument_id=instrument.id,
                    datetime=start_dt, # Using market open time as the timestamp for the day
                    price_not_found=True # Mark as not found initially
                )
                daily_records.append(daily_record)
                
                # Create Intraday Records (placeholders)
                for dt in five_minute_datetimes:
                    intraday_record = PriceHistoryIntraday(
                        instrument_id=instrument.id,
                        datetime=dt,
                        price_not_found=True # Mark as not found initially
                    )
                    intraday_records.append(intraday_record)

            # Bulk insert records
            if daily_records:
                self.session.add_all(daily_records)
            
            if intraday_records:
                self.session.add_all(intraday_records)
            
            await self.session.commit()

            logger.info(f"Successfully created {len(daily_records)} daily and {len(intraday_records)} intraday records for {exchange.exchange_name} on {target_date}")

        except Exception as e:
            logger.error(f"Error creating data records for {exchange.exchange_name}: {e!r}")
            await self.session.rollback()