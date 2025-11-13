import asyncio
import time
from typing import Dict, Any

from config.logger import logger
from config.redis_config import get_redis
from services.redis_helper import get_redis_helper
from services.websocket_manager import get_websocket_manager
from utils.ohlcv_to_binary import pack_ohlcv


class DataBroadcast:
    def __init__(self):
        self.redis = get_redis()
        self.redis_helper = get_redis_helper()
        self.websocket_manager = get_websocket_manager()
        self.broadcast_task = None

    async def start_broadcast(self):
        if self.broadcast_task and not self.broadcast_task.done():
            logger.warning("Broadcast task is already running.")
            return
        self.broadcast_task = asyncio.create_task(self.broadcast_prices())

    async def broadcast_prices(self):
        """
        Publish only when values change, using a snapshot to avoid race with writer threads.
        """
        while True:
            start = time.time()
            publish_tasks = []

            try:
                # Take a snapshot to avoid 'dict changed size during iteration' from threadpool writer
                snapshot: Dict[str, Any] = dict(
                    self.redis_helper.get_value("prices_dict")
                )
            except Exception as e:
                logger.warning(f"Snapshot failed: {e}")
                snapshot = {}

            # Filter snapshot to only active channels
            active_channels = self.websocket_manager.get_active_channels()
            snapshot = {k: v for k, v in snapshot.items() if k in active_channels}

            for channel, data in snapshot.items():
                timestamp = float(data.get("timestamp", time.time()))
                price = float(data.get("price", 0.0))
                volume = float(data.get("day_volume", 0.0))
                binary = pack_ohlcv(
                    timestamp=timestamp,
                    ohlcv=[price - 20, price + 50, price - 50, price, volume],
                )
                publish_tasks.append(self.redis.publish(channel, binary))

            # Add a fixed cadence sleep; runs in parallel with publishes,
            # so cadence ~= max(5s, publish time).
            publish_tasks.append(asyncio.sleep(3))

            # Await all
            result = await asyncio.gather(*publish_tasks, return_exceptions=True)

            # Log any publish errors
            for idx, res in enumerate(result):
                if isinstance(res, Exception):
                    logger.error(f"Publish task {idx} failed: {res}")

            # Optional: basic metrics
            sent = len(publish_tasks) - 1  # exclude sleep
            logger.info(
                f"Broadcast tick: sent={sent}, dt={round(time.time() - start, 3)}s"
            )
