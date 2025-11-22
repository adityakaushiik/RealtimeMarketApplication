import asyncio

from config.logger import logger
from services.provider.provider_manager import ProviderManager
from services.redis_timeseries import get_redis_timeseries
from services.websocket_manager import get_websocket_manager
from utils.common_constants import DataIngestionFormat


class LiveDataIngestion:
    def __init__(self):
        self._loop = None
        self.redis_timeseries = get_redis_timeseries()

        # NEW: Use ProviderManager instead of single provider
        self.provider_manager = ProviderManager(callback=self.handle_market_data)
        self._sync_task = None

    def handle_market_data(self, message: DataIngestionFormat):
        """Handle incoming market data from ANY provider and save to Redis."""
        try:
            # Schedule the async method in the main loop
            if self._loop and self._loop.is_running():
                asyncio.run_coroutine_threadsafe(
                    self._save_to_timeseries(**message.model_dump()), self._loop
                )
        except Exception as e:
            logger.error(f"Error handling market data: {e}")

    async def _save_to_timeseries(
        self, symbol: str, timestamp: int, price: float, volume: float, provider_code: str
    ):
        """Save data to timeseries (now includes provider_code for tracking)."""
        try:
            await self.redis_timeseries.add_to_timeseries(
                symbol, timestamp=timestamp, price=price, volume=volume
            )
            logger.debug(f"Saved {symbol} from {provider_code}: ${price}")
        except Exception as e:
            logger.error(f"Error saving to timeseries for {symbol}: {e}")

    async def start_ingestion(self):
        """Start data ingestion from all configured providers"""
        logger.info("🚀 Starting multi-provider data ingestion...")

        # Capture the main event loop
        try:
            self._loop = asyncio.get_running_loop()
        except RuntimeError:
            self._loop = asyncio.get_event_loop()

        # Initialize provider manager (loads exchange mappings from DB)
        await self.provider_manager.initialize()

        # Get symbols grouped by provider from database
        symbols_by_provider = await self.provider_manager.get_symbols_by_provider()

        if not symbols_by_provider:
            logger.warning("⚠️  No symbols found to subscribe to. Check database configuration.")
            return

        # Start all provider connections
        await self.provider_manager.start_all_providers(symbols_by_provider)

        logger.info("✅ Multi-provider data ingestion started successfully")

        # Optionally start dynamic subscription sync (commented out by default)
        # Uncomment to enable dynamic subscription management based on active clients
        # self._sync_task = asyncio.create_task(self.sync_subscriptions_with_clients())

    def stop_ingestion(self):
        """Stop all provider connections"""
        if self._sync_task:
            self._sync_task.cancel()
            logger.info("Stopped subscription sync task")

        self.provider_manager.stop_all_providers()
        logger.info("Data ingestion stopped.")

    def get_provider_status(self):
        """Get current status of all providers"""
        return self.provider_manager.get_provider_status()

    async def sync_subscriptions_with_clients(self):
        """
        Periodically sync provider subscriptions with active client channels.
        This ensures we only fetch data for symbols that clients are watching.

        OPTIONAL: Uncomment the call in start_ingestion() to enable this feature.
        """
        logger.info("Starting dynamic subscription sync...")

        while True:
            try:
                await asyncio.sleep(30)  # Run every 30 seconds

                # Get symbols clients are actively watching
                websocket_manager = get_websocket_manager()
                active_channels = websocket_manager.get_active_channels()

                # Get currently subscribed symbols across all providers
                current_subscriptions = set()
                for provider_code, provider in self.provider_manager.providers.items():
                    if provider:
                        current_subscriptions.update(provider.subscribed_symbols)

                # Calculate diff
                to_subscribe = active_channels - current_subscriptions
                to_unsubscribe = current_subscriptions - active_channels

                # Apply changes
                if to_subscribe:
                    await self.provider_manager.subscribe_to_symbols(list(to_subscribe))
                    logger.info(f"📈 Subscribed to {len(to_subscribe)} new symbols")

                if to_unsubscribe:
                    await self.provider_manager.unsubscribe_from_symbols(list(to_unsubscribe))
                    logger.info(f"📉 Unsubscribed from {len(to_unsubscribe)} symbols")

            except asyncio.CancelledError:
                logger.info("Subscription sync task cancelled")
                break
            except Exception as e:
                logger.error(f"Error syncing subscriptions: {e}")
