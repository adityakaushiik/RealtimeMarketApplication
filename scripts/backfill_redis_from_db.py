"""Backfill Redis with today's DB data for NIFTY (and other instruments)"""
import asyncio
import sys
import os
from datetime import datetime, timezone, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.database_config import get_db_session
from services.redis_timeseries import get_redis_timeseries
from sqlalchemy import select
from sqlalchemy.orm import joinedload
from models import PriceHistoryIntraday, Instrument


def _get_today_boundary_utc(exchange_tz: str) -> datetime:
    """Get today's start in UTC for a given exchange timezone."""
    import pytz

    tz = pytz.timezone(exchange_tz)
    now_in_tz = datetime.now(tz)
    today_start_in_tz = now_in_tz.replace(hour=0, minute=0, second=0, microsecond=0)
    return today_start_in_tz.astimezone(pytz.UTC).replace(tzinfo=timezone.utc)


async def backfill_redis_from_db():
    """
    For all recordable instruments, copy today's DB data to Redis.
    This is a one-time fix to sync Redis with DB for today's data.
    """
    print("=" * 60)
    print("BACKFILLING REDIS FROM DB FOR TODAY'S DATA")
    print("=" * 60)

    rts = get_redis_timeseries()

    async for session in get_db_session():
        # Get all recordable instruments
        result = await session.execute(
            select(Instrument)
            .options(joinedload(Instrument.exchange))
            .where(Instrument.should_record_data == True)
        )
        instruments = result.scalars().all()

        print(f"Found {len(instruments)} recordable instruments")

        total_records = 0

        for instrument in instruments:
            exchange_tz = instrument.exchange.timezone if instrument.exchange else "UTC"
            today_boundary_utc = _get_today_boundary_utc(exchange_tz)

            # Get today's data from DB
            result = await session.execute(
                select(PriceHistoryIntraday).where(
                    PriceHistoryIntraday.instrument_id == instrument.id,
                    PriceHistoryIntraday.datetime >= today_boundary_utc
                ).order_by(PriceHistoryIntraday.datetime)
            )
            records = result.scalars().all()

            if not records:
                continue

            # Add each record to Redis
            for r in records:
                try:
                    timestamp_ms = int(r.datetime.timestamp() * 1000)

                    await rts.add_5m_candle(
                        symbol=instrument.symbol,
                        timestamp_ms=timestamp_ms,
                        open_price=r.open or 0,
                        high_price=r.high or 0,
                        low_price=r.low or 0,
                        close_price=r.close or 0,
                        volume=r.volume or 0,
                    )
                    total_records += 1
                except Exception as e:
                    # Skip errors (likely duplicates)
                    pass

            print(f"  {instrument.symbol}: {len(records)} records")

        print(f"\nTotal records backfilled: {total_records}")
        break


async def verify_nifty_after_backfill():
    """Verify NIFTY data after backfill"""
    from config.redis_config import get_redis

    redis = get_redis()

    print("\n" + "=" * 60)
    print("VERIFYING NIFTY AFTER BACKFILL")
    print("=" * 60)

    # Check 5m close data
    market_open_utc = datetime(2026, 1, 20, 3, 45, 0, tzinfo=timezone.utc)
    market_close_utc = datetime(2026, 1, 20, 10, 30, 0, tzinfo=timezone.utc)

    from_ts = int(market_open_utc.timestamp() * 1000)
    to_ts = int(market_close_utc.timestamp() * 1000)

    try:
        close_key = "NIFTY:5m:close"
        data = await redis.execute_command("TS.RANGE", close_key, from_ts, to_ts)

        print(f"NIFTY:5m:close data points: {len(data) if data else 0}")

        if data:
            timestamps = [ts for ts, _ in data]
            min_ts = min(timestamps)
            max_ts = max(timestamps)

            min_dt = datetime.fromtimestamp(min_ts/1000, tz=timezone.utc)
            max_dt = datetime.fromtimestamp(max_ts/1000, tz=timezone.utc)

            print(f"First data point: {min_dt}")
            print(f"Last data point: {max_dt}")

            # Count expected 5min candles from market open
            expected_first = market_open_utc
            if min_dt <= expected_first + timedelta(minutes=5):
                print(f"✅ Data starts near market open!")
            else:
                print(f"⚠️ Still missing data from {expected_first} to {min_dt}")

    except Exception as e:
        print(f"Error: {e}")

    await redis.aclose()


async def main():
    await backfill_redis_from_db()
    await verify_nifty_after_backfill()


if __name__ == "__main__":
    asyncio.run(main())

