import asyncio

import redis

from config.logger import logger
from config.redis_config import get_redis

# from services.data.data_broadcast import get_data_broadcast
from services.provider.provider_manager import ProviderManager
from services.redis_timeseries import get_redis_timeseries
from services.websocket_manager import get_websocket_manager
from utils.binary_conversions import pack_snapshot, pack_update, example_update_data
from utils.common_constants import DataIngestionFormat


class LiveDataIngestion:
    def __init__(self):
        self._loop = None
        self.queue = asyncio.Queue()
        self._worker_task = None

        self.redis_timeseries = get_redis_timeseries()
        # self.data_broadcast = get_data_broadcast()

        self.websocket_manager = get_websocket_manager()
        self.redis = get_redis()

        # NEW: Use ProviderManager instead of single provider
        self.provider_manager = ProviderManager(callback=self.handle_market_data)
        self._sync_task = None

    def handle_market_data(self, message: DataIngestionFormat):
        """
        Handle incoming market data from ANY provider.
        Pushes data to an in-memory queue for async processing to avoid blocking the callback.
        """
        try:
            # Fast path: push to queue immediately
            # Resolve the symbol in the worker to keep this callback as fast as possible
            if self._loop and self._loop.is_running():
                self.queue.put_nowait(message)
        except Exception as e:
            logger.error(f"Error queuing market data: {e}")

    async def _process_queue(self):
        """
        Background worker to process market data from the queue.
        Batches writes to Redis for better performance.
        """
        logger.info("Starting data ingestion worker...")
        batch_size = 50  # Process up to 50 items at a time

        while True:
            try:
                # Fetch first item (blocking wait)
                message = await self.queue.get()
                batch = [message]

                # Try to fetch more items immediately if available (up to batch_size)
                try:
                    for _ in range(batch_size - 1):
                        batch.append(self.queue.get_nowait())
                except asyncio.QueueEmpty:
                    pass

                # Process the batch
                for msg in batch:
                    try:
                        await self._process_message(msg)
                    except Exception as e:
                        logger.error(f"Error processing message: {e}")
                    finally:
                        self.queue.task_done()

            except asyncio.CancelledError:
                logger.info("Data ingestion worker cancelled")
                break
            except Exception as e:
                logger.error(f"Fatal error in ingestion worker: {e}")
                await asyncio.sleep(1)  # Prevent tight loop on error

    async def _process_message(self, message: DataIngestionFormat):
        """Process a single message and save to Redis."""

        # Resolve internal instrument symbol using the sync method
        internal_symbol = self.provider_manager.get_internal_symbol_sync(
            message.provider_code, message.symbol
        )

        if internal_symbol:
            symbol_to_use = internal_symbol
        else:
            # Fallback logic
            if not self.provider_manager.provider_symbol_map:
                # Map might not be loaded yet
                pass
            symbol_to_use = message.symbol

        tasks = [
            # Save to timeseries
            self._save_to_timeseries(
                symbol=symbol_to_use,
                timestamp=message.timestamp,
                price=message.price,
                volume=message.volume,
                provider_code=message.provider_code,
            ),
            # Broadcast to clients
            self._broadcast_message(
                symbol=symbol_to_use,
                timestamp=message.timestamp,
                price=message.price,
                volume=message.volume,
                provider_code=message.provider_code,
            ),
        ]

        await asyncio.gather(*tasks, return_exceptions=True)

    async def _save_to_timeseries(
        self,
        symbol: str,
        timestamp: int,
        price: float,
        volume: float,
        provider_code: str,
    ):
        """Save data to timeseries (now includes provider_code for tracking)."""
        try:
            await self.redis_timeseries.add_to_timeseries(
                symbol, timestamp=timestamp, price=price, volume=volume
            )
            logger.debug(f"Saved {symbol} from {provider_code}: ${price}")
        except Exception as e:
            logger.error(f"Error saving to timeseries for {symbol}: {e!r}")

    async def _broadcast_message(
        self,
        symbol: str,
        timestamp: int,
        price: float,
        volume: float,
        provider_code: str,
    ):
        # No active clients for this symbol, skip broadcasting
        if symbol not in self.websocket_manager.get_active_channels():
            return

        try:
            # Broadcast via Redis Pub/Sub
            await self.redis.publish(
                symbol,
                pack_update(
                    {
                        "symbol": symbol,
                        "timestamp": timestamp,
                        "price": price,
                        "volume": int(volume),
                    }
                ),
            )
            logger.debug(f"Broadcasted {symbol} from {provider_code}: ${price}")
        except Exception as e:
            logger.error(f"Error broadcasting message for {symbol}: {e!r}")

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
            logger.warning(
                "⚠️  No symbols found to subscribe to. Check database configuration."
            )
            return

        # Start all provider connections
        await self.provider_manager.start_all_providers(symbols_by_provider)

        # Start the worker task
        self._worker_task = asyncio.create_task(self._process_queue())

        logger.info("✅ Multi-provider data ingestion started successfully")

        # Optionally start dynamic subscription sync (commented out by default)
        # Uncomment to enable dynamic subscription management based on active clients
        # self._sync_task = asyncio.create_task(self.sync_subscriptions_with_clients())

    def stop_ingestion(self):
        """Stop all provider connections"""
        if self._sync_task:
            self._sync_task.cancel()
            logger.info("Stopped subscription sync task")

        if self._worker_task:
            self._worker_task.cancel()
            logger.info("Stopped ingestion worker task")

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
                    await self.provider_manager.unsubscribe_from_symbols(
                        list(to_unsubscribe)
                    )
                    logger.info(f"📉 Unsubscribed from {len(to_unsubscribe)} symbols")

            except asyncio.CancelledError:
                logger.info("Subscription sync task cancelled")
                break
            except Exception as e:
                logger.error(f"Error syncing subscriptions: {e}")
