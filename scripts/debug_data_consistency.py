import asyncio
import logging
from datetime import datetime, timezone, timedelta
import pytz
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from config.database_config import get_db_session
from config.redis_config import get_redis
from services.redis_timeseries import get_redis_timeseries
from models import Instrument, PriceHistoryIntraday, PriceHistoryDaily
from features.marketdata.marketdata_service import get_price_history_intraday, get_price_history_daily

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def verify_data_consistency():
    """
    Script to verify data consistency between Redis, DB Intraday, and DB Daily.
    Specifically checks:
    1. Latest 5m candle from Redis is correctly merged into API response.
    2. Daily record updates correctly with intraday data.
    3. Volume accumulation logic.
    """

    # Target symbol for investigation
    target_symbol = "AXISBANK"
    exchange_code = "NSE" # Assuming NSE for AXISBANK

    logger.info(f"Starting investigation for {target_symbol} on {exchange_code}...")

    async for session in get_db_session():
        # 1. Get Instrument
        stmt = select(Instrument).where(Instrument.symbol == target_symbol)
        result = await session.execute(stmt)
        instrument = result.scalar_one_or_none()

        if not instrument:
            logger.error(f"Instrument {target_symbol} not found in DB.")
            return

        logger.info(f"Found Instrument: {instrument.symbol} (ID: {instrument.id})")

        # 2. Check Redis Data (Ongoing Candle)
        rts = get_redis_timeseries()
        redis_candle = await rts.get_ohlcv_last_5m(instrument.symbol)

        if redis_candle:
            ts = redis_candle['ts']
            dt_utc = datetime.fromtimestamp(ts / 1000, tz=timezone.utc)
            logger.info(f"Redis Latest Candle (Ongoing):")
            logger.info(f"  Time: {dt_utc} (TS: {ts})")
            logger.info(f"  OHLCV: {redis_candle['open']}, {redis_candle['high']}, {redis_candle['low']}, {redis_candle['close']}, {redis_candle['volume']}")
        else:
            logger.warning("No data found in Redis for this symbol.")

        # 3. Check Intraday API Response (Should include Redis candle)
        logger.info("Fetching Intraday History via Service...")
        intraday_history = await get_price_history_intraday(session, instrument.id, interval="5m")

        if intraday_history:
            latest_rec = intraday_history[0]
            logger.info(f"Latest Intraday Record from Service:")
            logger.info(f"  Time: {latest_rec.datetime}")
            logger.info(f"  OHLCV: {latest_rec.open}, {latest_rec.high}, {latest_rec.low}, {latest_rec.close}, {latest_rec.volume}")

            if redis_candle:
                # Verification
                redis_dt = datetime.fromtimestamp(redis_candle['ts'] / 1000, tz=timezone.utc)
                if latest_rec.datetime == redis_dt:
                    logger.info("✅ Service correctly merged Redis candle timestamp.")
                    if (latest_rec.close == redis_candle['close'] and
                        latest_rec.volume == int(redis_candle['volume'])):
                         logger.info("✅ Service correctly merged Redis candle data values.")
                    else:
                         logger.error("❌ Mismatch in data values between Redis and Service response!")
                         logger.error(f"Redis: C={redis_candle['close']}, V={redis_candle['volume']}")
                         logger.error(f"Service: C={latest_rec.close}, V={latest_rec.volume}")
                else:
                    if latest_rec.datetime > redis_dt:
                         logger.error("❌ Service returned a record NEWER than Redis? This shouldn't happen.")
                    else:
                         logger.warning("⚠️ Service latest record is OLDER than Redis. Redis merge might have failed or Redis data is too new.")

        else:
            logger.warning("No intraday history returned.")

        # 4. Check Daily API Response (Should include aggregated intraday + Redis)
        logger.info("Fetching Daily History via Service...")
        daily_history = await get_price_history_daily(session, instrument.id)

        if daily_history:
            today_rec = daily_history[0]
            logger.info(f"Latest Daily Record from Service:")
            logger.info(f"  Date: {today_rec.datetime}")
            logger.info(f"  OHLCV: {today_rec.open}, {today_rec.high}, {today_rec.low}, {today_rec.close}, {today_rec.volume}")

            # Verify Volume Accumulation
            # Sum of all intraday volumes for today
            today_start = today_rec.datetime
            today_end = today_start + timedelta(days=1)

            # Filter intraday records for today (excluding the one we just merged from Redis if it's not in DB yet)
            # Actually get_price_history_intraday returns merged list, so we can sum that directly

            daily_vol_sum = 0
            intraday_count = 0
            logger.info(f"Checking intraday records between {today_start} and {today_end}")

            for rec in intraday_history:
                if rec.datetime >= today_start and rec.datetime < today_end:
                    daily_vol_sum += rec.volume
                    intraday_count += 1
                    # Log first few records to verify
                    if intraday_count <= 5:
                        logger.info(f"  Intraday Rec: {rec.datetime} Vol: {rec.volume} Close: {rec.close}")

            logger.info(f"Found {intraday_count} intraday records for today.")
            logger.info(f"Sum of Intraday Volumes (from Service response): {daily_vol_sum}")

            if abs(daily_vol_sum - today_rec.volume) < 1: # Allow small diff
                logger.info("✅ Daily volume matches sum of intraday volumes.")
            else:
                logger.warning(f"⚠️ Daily volume mismatch! Daily: {today_rec.volume}, Sum: {daily_vol_sum}")

        else:
            logger.warning("No daily history returned.")

if __name__ == "__main__":
    asyncio.run(verify_data_consistency())
