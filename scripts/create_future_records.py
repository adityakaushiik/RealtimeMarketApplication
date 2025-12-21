import asyncio
import sys
import os
import logging

# Add project root to path
sys.path.append(os.getcwd())

from config.database_config import get_db_session
from services.data.data_creation import DataCreationService
from features.exchange.exchange_service import get_all_active_exchanges

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

async def main():
    offset_days = 1
    if len(sys.argv) > 1:
        try:
            offset_days = int(sys.argv[1])
        except ValueError:
            logger.error("Invalid offset_days argument. Using default 0.")

    logger.info(f"Starting data creation job with offset_days={offset_days}")

    async for session in get_db_session():
        try:
            # 1. Get all active exchanges
            exchanges = await get_all_active_exchanges(session)
            if not exchanges:
                logger.warning("No active exchanges found.")
                return

            # 2. Initialize DataCreationService
            service = DataCreationService(session)
            for exchange in exchanges:
                service.add_exchange(exchange)

            # 3. Run data creation
            await service.start_data_creation(offset_days=offset_days)

            logger.info("Data creation job completed successfully.")

        except Exception as e:
            logger.error(f"Data creation job failed: {e}", exc_info=True)
            # Exit with error code so cron knows it failed
            sys.exit(1)

if __name__ == "__main__":
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())

