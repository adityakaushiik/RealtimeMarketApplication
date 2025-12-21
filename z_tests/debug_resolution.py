import asyncio
import logging
import sys
import os
from datetime import datetime, timedelta, timezone
import pandas as pd

# Add project root to path
sys.path.append(os.getcwd())

from config.database_config import get_db_session
from config.logger import logger
from models import Instrument
from services.provider.dhan_provider import DhanProvider
from services.provider.yahoo_provider import YahooFinanceProvider
from services.provider.provider_manager import ProviderManager
from sqlalchemy import select

# Configure logging to stdout
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

async def get_instrument(symbol: str):
    async for session in get_db_session():
        stmt = select(Instrument).where(Instrument.symbol == symbol)
        result = await session.execute(stmt)
        return result.scalars().first()

async def debug_dhan_resolution():
    logger.info("="*50)
    logger.info("DEBUGGING DHAN RESOLUTION (HDFCBANK)")
    logger.info("="*50)

    # 1. Get Instrument
    symbol = "HDFCBANK" # Or whatever is in your DB
    instrument = await get_instrument(symbol)

    if not instrument:
        logger.error(f"Instrument {symbol} not found in DB. Trying 'HDFCBANK-EQ'...")
        symbol = "HDFCBANK-EQ"
        instrument = await get_instrument(symbol)

    if not instrument:
        logger.error("HDFCBANK not found. Aborting Dhan test.")
        return

    logger.info(f"Found Instrument: {instrument.symbol} (ID: {instrument.id}, Exchange: {instrument.exchange_id})")

    # 2. Initialize Provider
    # We need a mock provider manager or just pass None if we ensure symbol is correct
    # DhanProvider uses provider_manager to look up securityID if available.
    # If not, it uses instrument.symbol.
    # HDFCBANK on NSE is usually ID 1333.
    # If instrument.symbol is "HDFCBANK", Dhan API might fail if it expects "1333".
    # Let's try to initialize ProviderManager to get real mappings if possible.

    provider_manager = ProviderManager()
    # We won't call initialize() fully as it connects websockets, but we might need to load mappings?
    # Actually, let's just mock the lookup if needed, or rely on the provider's fallback.
    # For this test, let's assume we might need to manually set the symbol if it fails.

    dhan = DhanProvider(callback=lambda x: print(f"Callback: {x}"), provider_manager=provider_manager)

    # Manually inject mapping if needed for test (mocking what initialize would do)
    # provider_manager.provider_symbol_map = {"DHAN": {"HDFCBANK": "1333"}}
    # But get_search_code uses DB.

    # Let's just try. If it fails, we know why.

    # 3. Define Resolution Window
    # Let's pick a recent trading day.
    # Today is 2025-12-21 (Sunday). Last trading day was likely Friday 2025-12-19.
    # Let's try to fetch data for Friday 2025-12-19.

    target_date = datetime(2025, 12, 19, tzinfo=timezone.utc)
    start_time = target_date.replace(hour=4, minute=0) # 09:30 IST approx
    end_time = target_date.replace(hour=5, minute=0)   # 10:30 IST approx

    logger.info(f"Test Resolution Window (UTC): {start_time} to {end_time}")

    # 4. Fetch Intraday
    logger.info("--- Fetching Intraday Prices ---")
    try:
        # We pass a list of instruments
        results = await dhan.get_intraday_prices([instrument], start_date=start_time, end_date=end_time)

        if instrument.symbol in results:
            records = results[instrument.symbol]
            logger.info(f"Received {len(records)} intraday records.")
            if records:
                logger.info(f"Sample Record 1: {records[0].datetime} (UTC) | Open: {records[0].open}")
                logger.info(f"Sample Record -1: {records[-1].datetime} (UTC) | Open: {records[-1].open}")

                # Verify Timestamp
                # 09:15 IST is 03:45 UTC.
                # If we requested 04:00 UTC (09:30 IST), we expect timestamps around there.
                first_ts = records[0].datetime
                logger.info(f"First returned timestamp: {first_ts}")

                # Check if it looks like valid UTC
                # If it was 09:30 IST, it should be 04:00 UTC.
                # If the bug exists (double subtraction), it might be 04:00 - 5.5h = 22:30 (previous day).
                if first_ts.hour == 4 or first_ts.hour == 3:
                    logger.info("✅ Timestamp looks correct (UTC).")
                else:
                    logger.warning(f"❌ Timestamp might be incorrect! Expected ~04:00 UTC, got {first_ts.hour}:{first_ts.minute}")

        else:
            logger.warning("No data returned for symbol.")

    except Exception as e:
        logger.error(f"Error fetching intraday: {e}", exc_info=True)

    # 5. Fetch Daily
    logger.info("--- Fetching Daily Prices ---")
    try:
        # Daily usually takes dates.
        daily_start = target_date - timedelta(days=5)
        daily_end = target_date

        results = await dhan.get_daily_prices([instrument], start_date=daily_start, end_date=daily_end)

        if instrument.symbol in results:
            records = results[instrument.symbol]
            logger.info(f"Received {len(records)} daily records.")
            for r in records:
                logger.info(f"Date: {r.datetime} | Close: {r.close}")
        else:
            logger.warning("No daily data returned.")

    except Exception as e:
        logger.error(f"Error fetching daily: {e}", exc_info=True)


async def debug_yf_resolution():
    logger.info("\n" + "="*50)
    logger.info("DEBUGGING YFINANCE RESOLUTION (AAPL)")
    logger.info("="*50)

    # 1. Get Instrument
    symbol = "AAPL"
    instrument = await get_instrument(symbol)

    if not instrument:
        logger.error("AAPL not found in DB. Aborting YF test.")
        return

    logger.info(f"Found Instrument: {instrument.symbol} (ID: {instrument.id}, Exchange: {instrument.exchange_id})")

    # 2. Initialize Provider
    yf_provider = YahooFinanceProvider(callback=lambda x: print(f"Callback: {x}"))

    # 3. Define Resolution Window
    # Friday 2025-12-19. Market open 09:30 EST = 14:30 UTC.
    target_date = datetime(2025, 12, 19, tzinfo=timezone.utc)
    start_time = target_date.replace(hour=14, minute=30)
    end_time = target_date.replace(hour=15, minute=30)

    logger.info(f"Test Resolution Window (UTC): {start_time} to {end_time}")

    # 4. Fetch Intraday
    logger.info("--- Fetching Intraday Prices ---")
    try:
        results = await yf_provider.get_intraday_prices([instrument], start_date=start_time, end_date=end_time)

        if instrument.symbol in results:
            records = results[instrument.symbol]
            logger.info(f"Received {len(records)} intraday records.")
            if records:
                logger.info(f"Sample Record 1: {records[0].datetime} (UTC) | Open: {records[0].open} | InstID: {records[0].instrument_id}")

                # Check ID fix
                if records[0].instrument_id == 0:
                    logger.info("ℹ️ Instrument ID is 0. This is expected in direct provider calls. DataResolver handles mapping.")
                else:
                    logger.info(f"✅ Instrument ID is {records[0].instrument_id}")

                # Verify Timestamp
                # 14:30 UTC.
                first_ts = records[0].datetime
                logger.info(f"First returned timestamp: {first_ts}")

                if first_ts.hour == 14 and first_ts.minute == 30:
                    logger.info("✅ Timestamp looks correct (UTC).")
                else:
                    logger.warning(f"❌ Timestamp might be incorrect! Expected 14:30 UTC, got {first_ts.hour}:{first_ts.minute}")

        else:
            logger.warning("No data returned for symbol.")

    except Exception as e:
        logger.error(f"Error fetching intraday: {e}", exc_info=True)

    # 5. Fetch Daily
    logger.info("--- Fetching Daily Prices ---")
    try:
        daily_start = target_date - timedelta(days=5)
        daily_end = target_date

        results = await yf_provider.get_daily_prices([instrument], start_date=daily_start, end_date=daily_end)

        if instrument.symbol in results:
            records = results[instrument.symbol]
            logger.info(f"Received {len(records)} daily records.")
            for r in records:
                logger.info(f"Date: {r.datetime} | Close: {r.close}")
        else:
            logger.warning("No daily data returned.")

    except Exception as e:
        logger.error(f"Error fetching daily: {e}", exc_info=True)

async def main():
    await debug_dhan_resolution()
    await debug_yf_resolution()

if __name__ == "__main__":
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())

