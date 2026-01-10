import asyncio

from config.logger import logger
from config.redis_config import get_redis
from services.data.data_queue import get_data_ingestion_queue_instance, DataIngestionQueue
from services.provider.provider_manager import get_provider_manager
from services.redis_timeseries import get_redis_timeseries
from services.websocket_manager import get_websocket_manager
from utils.binary_conversions import pack_update
from utils.common_constants import DataIngestionFormat


class LiveDataIngestion:
    def __init__(self):
        self._loop = None
        self.queue : DataIngestionQueue = get_data_ingestion_queue_instance()  # Use the shared singleton queue
        self._worker_task = None
        self._stats_task = None  # Added stats task

        self.redis_timeseries = get_redis_timeseries()
        self.websocket_manager = get_websocket_manager()
        self.redis = get_redis()

        # NEW: Use ProviderManager instead of single provider
        self.provider_manager = None
        if not get_provider_manager():
            self.provider_manager = get_provider_manager()
        
        # Stats tracking
        self.stats_processed_count = 0
        self.stats_symbol_counts = {}

        # Cache for volume delta calculation
        self.last_volumes = {}

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
                message = await self.queue.get_data()
                batch = [message]

                # Try to fetch more items immediately if available (up to batch_size)
                try:
                    for _ in range(batch_size - 1):
                        batch.append(self.queue.queue.get_nowait())
                        self.queue.queue.task_done()
                except asyncio.QueueEmpty:
                    pass

                # Process the batch
                for msg in batch:
                    try:
                        await self._process_message(msg)
                    except Exception as e:
                        logger.error(f"Error processing message: {e}")
                    finally:
                        # task_done handled above for batching
                        pass

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

        # NOTE: Providers now push NORMALIZED internal symbols to the queue.
        # No need to resolve here anymore.
        symbol_to_use = message.symbol

        # Calculate Volume Delta (Tick Size)
        current_volume = int(message.volume)
        last_volume = self.last_volumes.get(symbol_to_use)

        if last_volume is None:
            # First tick seen since restart.
            # If we assume 0, we might get a huge spike equal to daily volume.
            # If we assume current, we miss the volume of this specific tick.
            # Safety choice: Assume current_volume is the baseline, delta is 0 for this first tick.
            tick_volume = 0
        else:
            tick_volume = current_volume - last_volume
            if tick_volume < 0:
                # Data correction or reset event on provider side
                tick_volume = 0

        # Update cache
        self.last_volumes[symbol_to_use] = current_volume

        tasks = [
            # Save to timeseries
            self._save_to_timeseries(
                symbol=symbol_to_use,
                timestamp=message.timestamp,
                price=message.price,
                volume=message.volume, # Storing Cumulative in DB is usually correct for consistency
                provider_code=message.provider_code,
            ),
            # Broadcast to clients
            self._broadcast_message(
                symbol=symbol_to_use,
                timestamp=message.timestamp,
                price=message.price,
                volume=message.volume, # Total Daily Volume (for UI Labels)
                tick_volume=tick_volume, # Delta Volume (for Charts/Candles)
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

        if timestamp <= 0:
            return

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
        tick_volume: int = 0,
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
                        "size": int(tick_volume), # "size" is a standard name for trade quantity
                    }
                ),
            )
            # logger.debug(f"Broadcasted {symbol} from {provider_code}: ${price} (Vol: {volume}, Delta: {tick_volume})")
        except Exception as e:
            logger.error(f"Error broadcasting message for {symbol}: {e!r}")

    async def start_ingestion(self):
        """Start the data ingestion process."""
        self._loop = asyncio.get_running_loop()

        # Start stats task
        self._stats_task = asyncio.create_task(self._log_stats())

        # Start processing worker
        self._worker_task = asyncio.create_task(self._process_queue())

        logger.info("✅ Multi-provider data ingestion started successfully")

    def stop_ingestion(self):
        """Stop all provider connections"""
        if self._stats_task:
            self._stats_task.cancel()
            logger.info("Stopped stats logging task")

        # Stop sync task via manager
        self.provider_manager.stop_sync_task()

        if self._worker_task:
            self._worker_task.cancel()
            logger.info("Stopped ingestion worker task")

        self.provider_manager.stop_all_providers()
        logger.info("Data ingestion stopped.")

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
                # if count > 0:
                #     logger.info(f"   Top symbols: {top_symbols}")

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error logging stats: {e}")
