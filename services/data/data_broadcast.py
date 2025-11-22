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
        Falls back to no publish if no active channels or no data.
        """
        while True:
            start = time.time()
            publish_tasks = []

            # Get currently active subscription channels (symbols)
            try:
                active_channels = self.websocket_manager.get_active_channels()
            except Exception as e:
                logger.error(f"Failed to get active channels: {e}")
                active_channels = []

            if not active_channels:
                # Nothing to broadcast; sleep cadence and continue
                await asyncio.sleep(3)
                logger.info("Broadcast tick: no active channels")
                continue

            # Fetch latest prices & volumes for active channels
            try:
                last_points = await self.redis_ts.get_multiple_last_traded_prices(active_channels)
            except Exception as e:
                logger.error(f"Failed to fetch last traded prices: {e}")
                last_points = []

            if not last_points:
                # Sleep and continue if no data returned
                await asyncio.sleep(3)
                logger.info("Broadcast tick: no data for active channels")
                continue

            # Build publish tasks
            for point in last_points:
                publish_tasks.append(self.redis.publish(point.symbol, json.dumps(point.model_dump())))

            # Add fixed cadence sleep; runs concurrently with publishes
            publish_tasks.append(asyncio.sleep(3))

            # Execute all publish tasks in parallel
            results = await asyncio.gather(*publish_tasks, return_exceptions=True)

            # Log any publish errors (exclude last item which is sleep)
            error_count = 0
            for idx, res in enumerate(results[:-1]):
                if isinstance(res, Exception):
                    error_count += 1
                    logger.error(f"Publish task {idx} failed: {res}")

            sent = len(publish_tasks) - 1  # exclude sleep task
            logger.info(
                f"Broadcast tick: sent={sent}, errors={error_count}, dt={round(time.time() - start, 3)}s"
            )
