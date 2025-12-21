from datetime import datetime, timedelta, time
from typing import List
import pytz

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from config.logger import logger
from models.exchange import Exchange
from models.exchange_holiday import ExchangeHoliday
from models.instruments import Instrument
from models.price_history_intraday import PriceHistoryIntraday
from models.price_history_daily import PriceHistoryDaily


class DataCreationService:
    def __init__(self, session: AsyncSession):
        self.exchanges: List[Exchange] = []
        self.session: AsyncSession = session

    def add_exchange(self, exchange: Exchange) -> None:
        """Add an exchange to monitor for data collection."""
        self.exchanges.append(exchange)
        logger.info(f"Added exchange: {exchange.name}")

    def list_exchanges(self) -> List[str]:
        """List the names of all exchanges being monitored."""
        return [exchange.name for exchange in self.exchanges]

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

    async def _create_data_records_for_exchange(
        self, exchange: Exchange, offset_days: int = 0
    ) -> None:
        """Create data records for a specific exchange."""
        # Refresh the exchange object to ensure attributes are loaded and not expired
        # This is necessary because previous iterations might have called commit(), expiring all objects.
        await self.session.refresh(exchange)

        exchange_name = exchange.name
        try:
            # Calculate target date and times
            tz = pytz.timezone(exchange.timezone)
            now = datetime.now(tz)
            target_date = now.date() + timedelta(days=offset_days)

            # Check for holidays or special sessions
            stmt_holiday = select(ExchangeHoliday).where(
                ExchangeHoliday.exchange_id == exchange.id,
                ExchangeHoliday.date == target_date
            )
            result_holiday = await self.session.execute(stmt_holiday)
            holiday = result_holiday.scalar_one_or_none()

            market_open = exchange.market_open_time
            market_close = exchange.market_close_time

            if holiday:
                if holiday.is_closed:
                    logger.info(f"Skipping data creation for {exchange_name} on {target_date}: Holiday ({holiday.description})")
                    return
                else:
                    # Special session (e.g. Muhurat trading)
                    if holiday.open_time and holiday.close_time:
                        logger.info(f"Using special trading hours for {exchange_name} on {target_date}: {holiday.open_time} - {holiday.close_time} ({holiday.description})")
                        market_open = holiday.open_time
                        market_close = holiday.close_time

            # Calculate start and end times for the target date
            # Use strictly market open and close times, ignoring pre/post market sessions
            if not market_open or not market_close:
                logger.warning(
                    f"Skipping data creation for {exchange_name}: Market open/close times not defined."
                )
                return

            start_dt = tz.localize(datetime.combine(target_date, market_open))
            end_dt = tz.localize(datetime.combine(target_date, market_close))

            logger.info(
                f"Creating data records for {exchange_name} on {target_date} (Offset: {offset_days})"
            )

            # Fetch active instruments for this exchange
            stmt = select(Instrument).where(
                Instrument.exchange_id == exchange.id,
                Instrument.should_record_data == True,
            )
            result = await self.session.execute(stmt)
            instruments = result.scalars().all()

            if not instruments:
                logger.warning(
                    f"No active instruments found for {exchange_name}"
                )
                return

            # Generate 5-minute intervals
            five_minute_datetimes = []
            current_dt = start_dt
            while current_dt <= end_dt:
                five_minute_datetimes.append(current_dt)
                current_dt += timedelta(minutes=exchange.interval_minutes)

            # Use market open time for daily record timestamp (standard convention)
            daily_record_dt = tz.localize(
                datetime.combine(target_date, market_open)
            )

            # Fetch existing records to avoid duplicates
            # Check for existing daily records for this SPECIFIC timestamp
            existing_daily_stmt = select(PriceHistoryDaily.instrument_id).where(
                PriceHistoryDaily.instrument_id.in_([i.id for i in instruments]),
                PriceHistoryDaily.datetime == daily_record_dt,
            )
            existing_daily_result = await self.session.execute(existing_daily_stmt)
            existing_daily_ids = set(existing_daily_result.scalars().all())

            # Check for existing intraday records (skip if any exist for the day to avoid partial insertion complexity)
            # NOTE: This means if you are trying to backfill pre-market data for a day that already has market data,
            # this will skip it. You would need to clear the day's data to regenerate everything.
            existing_intraday_stmt = (
                select(PriceHistoryIntraday.instrument_id)
                .where(
                    PriceHistoryIntraday.instrument_id.in_([i.id for i in instruments]),
                    PriceHistoryIntraday.datetime
                    >= tz.localize(datetime.combine(target_date, time.min)),
                    PriceHistoryIntraday.datetime
                    < tz.localize(
                        datetime.combine(target_date + timedelta(days=1), time.min)
                    ),
                )
                .group_by(PriceHistoryIntraday.instrument_id)
            )
            existing_intraday_result = await self.session.execute(
                existing_intraday_stmt
            )
            existing_intraday_ids = set(existing_intraday_result.scalars().all())

            intraday_records = []
            daily_records = []

            for instrument in instruments:
                # Create Daily Record if it doesn't exist at the correct timestamp
                if instrument.id not in existing_daily_ids:
                    daily_record = PriceHistoryDaily(
                        instrument_id=instrument.id,
                        datetime=daily_record_dt,
                        resolve_required=True,
                    )
                    daily_records.append(daily_record)

                # Create Intraday Records if they don't exist
                if instrument.id not in existing_intraday_ids:
                    for dt in five_minute_datetimes:
                        intraday_record = PriceHistoryIntraday(
                            instrument_id=instrument.id,
                            datetime=dt,
                            resolve_required=True,
                        )
                        intraday_records.append(intraday_record)

            # Bulk insert records
            if daily_records:
                self.session.add_all(daily_records)
                logger.info(f"Adding {len(daily_records)} new daily records.")

            if intraday_records:
                self.session.add_all(intraday_records)
                logger.info(f"Adding {len(intraday_records)} new intraday records.")

            if not daily_records and not intraday_records:
                logger.info(
                    f"No new records to create for {exchange_name} on {target_date}"
                )
            else:
                await self.session.commit()
                logger.info(
                    f"Successfully created records for {exchange_name} on {target_date}"
                )

        except Exception as e:
            logger.error(
                f"Error creating data records for {exchange_name}: {e!r}"
            )
            await self.session.rollback()
