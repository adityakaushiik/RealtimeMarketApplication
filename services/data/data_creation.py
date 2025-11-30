import asyncio
from typing import List, Optional

from sqlalchemy.ext.asyncio import AsyncSession

from config.logger import logger
from services.data.exchange_data import ExchangeData


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

    async def start_data_creation(self) -> None:
        """
        Create Data Records for the future for all monitored exchanges.
        this includes record for intraday and daily data.
        """

        tasks = [
            self._create_data_records_for_exchange(exchange)
            for exchange in self.exchanges
        ]

        # Start all tasks concurrently
        await asyncio.gather(*tasks, return_exceptions=True)

    async def _create_data_records_for_exchange(self, exchange:ExchangeData, start_time = None, end_time = None) -> None:
        """Create data records for a specific exchange."""
        try:

            ## Also add logic to put previous close into consideration while creating data records

            five_minute_datetime_for_exchange = []

            # Create 5-minute intervals from start_time to end_time
            current_time = exchange.start_time if start_time is None else start_time
            end_time = exchange.end_time if end_time is None else end_time

            while current_time <= end_time:
                five_minute_datetime_for_exchange.append(current_time)
                current_time += exchange.interval_minutes * 60 * 1000  # Convert minutes to milliseconds


            logger.info(f"Created {len(five_minute_datetime_for_exchange)} data records for {exchange.exchange_name}")

        except Exception as e:
            logger.error(f"Error creating data records for {exchange.exchange_name}: {e!r}")