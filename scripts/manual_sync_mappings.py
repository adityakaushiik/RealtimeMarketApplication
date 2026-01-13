import asyncio
import sys
import os

# Add project root to path
sys.path.append(os.getcwd())

from services.data.redis_mapping import get_redis_mapping_helper
from config.logger import logger
import logging

# Config logger to stdout
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.INFO)
logger.addHandler(handler)

async def sync():
    print("Starting manual sync...")
    mapper = get_redis_mapping_helper()
    await mapper.sync_mappings_from_db()
    print("Sync complete.")

if __name__ == "__main__":
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(sync())

