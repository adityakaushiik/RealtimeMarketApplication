import asyncio

import redis

from config.logger import logger
from config.redis_config import get_redis

# from services.data.data_broadcast import get_data_broadcast
from services.provider.provider_manager import ProviderManager, get_provider_manager
from services.redis_timeseries import get_redis_timeseries
from services.websocket_manager import get_websocket_manager
from utils.binary_conversions import pack_snapshot, pack_update, example_update_data
from utils.common_constants import DataIngestionFormat

from sqlalchemy import select
from models import Instrument
from config.database_config import get_db_session


class LiveDataIngestion:
    def __init__(self):
        self._loop = None
        self.queue = asyncio.Queue()
        self._worker_task = None
        self._stats_task = None  # Added stats task

        self.redis_timeseries = get_redis_timeseries()
        # self.data_broadcast = get_data_broadcast()

        self.websocket_manager = get_websocket_manager()
        self.redis = get_redis()

        # NEW: Use ProviderManager instead of single provider
        self.provider_manager = None
        if not get_provider_manager():
            self.provider_manager = get_provider_manager()
        
        self._sync_task = None
        self.persistent_symbols = set()
        self._subscription_change_event = asyncio.Event()

        # Stats tracking
        self.stats_processed_count = 0
        self.stats_symbol_counts = {}

    def _on_subscription_change(self):
        """Callback for when websocket subscriptions change."""
        if self._loop:
            self._loop.call_soon_threadsafe(self._subscription_change_event.set)

    async def _load_persistent_symbols(self):
        """Load symbols that should always be recorded."""
        async for session in get_db_session():
            result = await session.execute(
                select(Instrument.symbol).where(Instrument.should_record_data == True)
            )
            self.persistent_symbols = set(result.scalars().all())
            logger.info(f"Loaded {len(self.persistent_symbols)} persistent symbols")

    def handle_market_data(self, message: DataIngestionFormat):
        """
        Handle incoming market data from ANY provider.
        Pushes data to an in-memory queue for async processing to avoid blocking the callback.
        """
        try:
            # Fast path: push to queue immediately
            # Resolve the symbol in the worker to keep this callback as fast as possible
            if self._loop and self._loop.is_running():
                self._loop.call_soon_threadsafe(self.queue.put_nowait, message)
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

        # Update stats
        self.stats_processed_count += 1
        self.stats_symbol_counts[message.symbol] = self.stats_symbol_counts.get(message.symbol, 0) + 1

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

            # DEBUG: Log why mapping failed for a few samples
            if self.stats_processed_count % 100 == 0:
                logger.warning(f"Mapping failed for {message.provider_code}:{message.symbol}. Map size: {len(self.provider_manager.provider_symbol_map.get(message.provider_code, {}))}")
                # Log a few keys from the map to see format
                if self.provider_manager.provider_symbol_map.get(message.provider_code):
                    keys = list(self.provider_manager.provider_symbol_map[message.provider_code].keys())[:5]
                    logger.info(f"Sample keys in map: {keys}")

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

        # Register callback for subscription changes
        self.websocket_manager.register_callback(self._on_subscription_change)

        # Load persistent symbols
        await self._load_persistent_symbols()

        # Get symbols grouped by provider from database
        # This also populates the ProviderManager's internal mappings for all symbols
        all_symbols_by_provider = await self.provider_manager.get_symbols_by_provider()

        # Filter for initial subscription (only persistent symbols)
        # We only want to subscribe to symbols that are marked as should_record_data=True on startup
        initial_symbols_by_provider = {}
        
        # We need to map persistent symbols (internal symbols) to provider search codes
        # The persistent_symbols set contains internal symbols (e.g. "RELIANCE")
        # But symbols_by_provider contains provider search codes (e.g. "1333" or "RELIANCE-EQ")
        # We need to check if the provider search code maps to a persistent internal symbol
        
        for provider_code, search_codes in all_symbols_by_provider.items():
            initial_symbols_by_provider[provider_code] = []
            for search_code in search_codes:
                # Resolve internal symbol
                internal_symbol = self.provider_manager.get_internal_symbol_sync(provider_code, search_code)
                if internal_symbol and internal_symbol in self.persistent_symbols:
                    initial_symbols_by_provider[provider_code].append(search_code)

        if not all_symbols_by_provider:
            logger.warning(
                "⚠️  No symbols found to subscribe to. Check database configuration."
            )
            # Even if no symbols found initially, we should start the worker and sync task
            # return

        # Start all provider connections with ONLY persistent symbols
        if initial_symbols_by_provider:
            await self.provider_manager.start_all_providers(initial_symbols_by_provider)

        # Start the worker task
        self._worker_task = asyncio.create_task(self._process_queue())

        # Start stats logging task
        self._stats_task = asyncio.create_task(self._log_stats())

        logger.info("✅ Multi-provider data ingestion started successfully")

        # Start dynamic subscription sync
        self._sync_task = asyncio.create_task(self.sync_subscriptions_with_clients())

    def stop_ingestion(self):
        """Stop all provider connections"""
        if self._stats_task:
            self._stats_task.cancel()
            logger.info("Stopped stats logging task")

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
        This ensures we only fetch data for symbols that clients are watching OR are marked for recording.
        """
        logger.info("Starting dynamic subscription sync...")

        while True:
            try:
                # Wait for event or timeout (heartbeat)
                try:
                    await asyncio.wait_for(self._subscription_change_event.wait(), timeout=10.0)
                except asyncio.TimeoutError:
                    pass
                
                self._subscription_change_event.clear()

                # Get symbols clients are actively watching
                websocket_manager = get_websocket_manager()
                active_channels = websocket_manager.get_active_channels()

                # Combine active client channels with persistent symbols
                # Note: active_channels contains internal symbols (e.g. "RELIANCE")
                # persistent_symbols contains internal symbols (e.g. "RELIANCE")
                target_internal_symbols = active_channels.union(self.persistent_symbols)

                # We need to convert target internal symbols to provider search codes to compare with current subscriptions
                # current_subscriptions contains provider search codes (e.g. "1333")
                
                target_subscriptions = set()
                for internal_symbol in target_internal_symbols:
                    # Find provider search code for this internal symbol
                    # We can use the provider manager to find the provider and then the search code
                    # But ProviderManager doesn't have a direct reverse lookup "internal -> search code" easily accessible
                    # except via iterating or if we add it.
                    # However, we can iterate over all providers and their mappings.
                    
                    # Optimization: We can cache this reverse mapping or just iterate since it's not too frequent (every 10s)
                    # Let's iterate over the provider_symbol_map in ProviderManager
                    
                    found = False
                    for provider_code, mapping in self.provider_manager.provider_symbol_map.items():
                        for search_code, mapped_internal in mapping.items():
                            if mapped_internal == internal_symbol:
                                target_subscriptions.add(search_code)
                                found = True
                                break
                        if found:
                            break
                
                # Get currently subscribed symbols across all providers
                current_subscriptions = set()
                for provider_code, provider in self.provider_manager.providers.items():
                    if provider:
                        current_subscriptions.update(provider.subscribed_symbols)

                # Calculate diff
                to_subscribe = target_subscriptions - current_subscriptions
                to_unsubscribe = current_subscriptions - target_subscriptions

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

    async def _log_stats(self):
        """Log ingestion statistics every 10 seconds."""
        while True:
            try:
                await asyncio.sleep(10)

                q_size = self.queue.qsize()
                count = self.stats_processed_count
                unique_symbols = len(self.stats_symbol_counts)
                active_tasks = len(asyncio.all_tasks())

                # Get top 5 active symbols
                top_symbols = sorted(self.stats_symbol_counts.items(), key=lambda x: x[1], reverse=True)[:5]

                # Reset counters
                self.stats_processed_count = 0
                self.stats_symbol_counts = {}

                logger.info(f"📊 Ingestion Stats (10s): Queue={q_size}, Processed={count}, UniqueSymbols={unique_symbols}, ActiveTasks={active_tasks}")
                if count > 0:
                    logger.info(f"   Top symbols: {top_symbols}")

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error logging stats: {e}")
