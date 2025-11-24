import asyncio
import json
import time

from config.logger import logger
from config.redis_config import get_redis
from services.redis_timeseries import RedisTimeSeries, get_redis_timeseries
from services.websocket_manager import get_websocket_manager


class DataBroadcast:
    def __init__(self):
        self.redis = get_redis()
        self.websocket_manager = get_websocket_manager()
        self.broadcast_task = None
        self.redis_ts = get_redis_timeseries()  # Added: for fetching last traded prices

    async def start_broadcast(self):
        if self.broadcast_task and not self.broadcast_task.done():
            logger.warning("Broadcast task is already running.")
            return
        self.broadcast_task = asyncio.create_task(self.broadcast_prices())

    async def broadcast_prices(self):
        """
        Broadcast latest traded prices (timestamp, price, volume) only for active channels.
        Uses RedisTimeSeries.get_multiple_last_traded_prices for efficient concurrent fetch.
        """
        logger.info("Starting price broadcast loop...")
        while True:
            start_time = time.time()
            
            try:
                # Get currently active subscription channels (symbols)
                # Convert to list because redis_timeseries expects a sequence it can index into
                active_channels = list(self.websocket_manager.get_active_channels())
                
                if not active_channels:
                    # No active clients, sleep and retry
                    await asyncio.sleep(3)
                    continue

                # Fetch latest prices & volumes for active channels
                last_points = await self.redis_ts.get_multiple_last_traded_prices(active_channels)
                
                if not last_points:
                    await asyncio.sleep(1)
                    continue

                # Create publish tasks
                publish_tasks = [
                    self.redis.publish(point.symbol, json.dumps(point.model_dump()))
                    for point in last_points
                ]

                if publish_tasks:
                    await asyncio.gather(*publish_tasks, return_exceptions=True)

                # Calculate time taken and sleep for the remainder of the interval
                elapsed = time.time() - start_time
                sleep_time = max(0.1, 3.0 - elapsed) # Ensure at least 3s interval
                
                logger.debug(f"Broadcast: sent {len(publish_tasks)} updates in {elapsed:.3f}s")
                await asyncio.sleep(sleep_time)

            except asyncio.CancelledError:
                logger.info("Broadcast task cancelled")
                break
            except Exception as e:
                logger.error(f"Error in broadcast loop: {e}")
                await asyncio.sleep(3)  # Backoff on error
