import asyncio

from fastapi.concurrency import run_in_threadpool
from sqlalchemy import select

from config.database_config import get_db_session
from config.logger import logger
from models import ProviderInstrumentMapping
from services.redis_helper import get_redis_helper
from services.redis_timeseries import RedisTimeSeries
from services.yahoo_finance_connection import YahooFinanceProvider
from utils.common_constants import DataIngestionFormat


class LiveDataIngestion:
    def __init__(self):
        self.indian_tickers = None
        self._loop = None

        # Multiple Sources will be configured here in future
        self.redis_helper = get_redis_helper()
        self.data_provider = YahooFinanceProvider(callback=self.handle_market_data)
        self.redis_timeseries = RedisTimeSeries()
        self.prices_dict = None

    def handle_market_data(self, message: DataIngestionFormat):
        """Handle incoming market data and save to prices_dict in Redis."""
        try:
            # Schedule the async method in the main loop
            if self._loop and self._loop.is_running():
                asyncio.run_coroutine_threadsafe(
                    self._save_to_timeseries(**message.model_dump()), self._loop
                )
        except Exception as e:
            logger.error(f"Error handling market data: {e}")

    async def _save_to_timeseries(
        self, symbol: str, timestamp: int, price: float, volume: float
    ):
        """Save data to timeseries."""
        try:
            await self.redis_timeseries.add_to_timeseries(
                symbol, timestamp=timestamp, price=price, volume=volume
            )
        except Exception as e:
            logger.error(f"Error saving to timeseries for {symbol}: {e}")

    async def start_ingestion(self):
        # Get the list of Indian tickers from the database
        async for session in get_db_session():
            result = await session.execute(
                select(ProviderInstrumentMapping.provider_instrument_search_code)
            )
            self.indian_tickers = result.fetchall()
            self.indian_tickers = [row[0] for row in self.indian_tickers]

        # Capture the main event loop
        try:
            self._loop = asyncio.get_running_loop()
        except RuntimeError:
            self._loop = asyncio.get_event_loop()

        selected_tickers = self.indian_tickers.copy()
        await run_in_threadpool(
            self.data_provider.connect_websocket,
            selected_tickers,
        )

    def stop_ingestion(self):
        self.data_provider.disconnect_websocket()
        logger.info("Data ingestion stopped.")
