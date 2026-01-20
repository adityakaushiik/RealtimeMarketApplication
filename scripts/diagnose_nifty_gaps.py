"""Diagnose NIFTY data gaps - check both Redis and DB"""
import asyncio
import sys
import os
from datetime import datetime, timezone, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.redis_config import get_redis
from config.database_config import get_db_session
from sqlalchemy import select, func
from models import PriceHistoryIntraday, Instrument


async def check_redis_data():
    """Check what data is in Redis for NIFTY"""
    redis = get_redis()

    print("=" * 60)
    print("REDIS DATA FOR NIFTY")
    print("=" * 60)

    # Get today's date range in IST (market hours 9:15 AM - 3:30 PM IST)
    # IST is UTC+5:30
    # Market open: 9:15 IST = 3:45 UTC
    # Market close: 15:30 IST = 10:00 UTC

    today_utc = datetime(2026, 1, 20, 0, 0, 0, tzinfo=timezone.utc)
    market_open_utc = datetime(2026, 1, 20, 3, 45, 0, tzinfo=timezone.utc)  # 9:15 IST
    market_close_utc = datetime(2026, 1, 20, 10, 0, 0, tzinfo=timezone.utc)  # 15:30 IST

    from_ts = int(market_open_utc.timestamp() * 1000)
    to_ts = int(market_close_utc.timestamp() * 1000)

    try:
        # Get 5m close data
        close_key = "NIFTY:5m:close"
        data = await redis.execute_command("TS.RANGE", close_key, from_ts, to_ts)

        print(f"\nExpected market hours: {market_open_utc} to {market_close_utc}")
        print(f"Redis data points for NIFTY:5m:close: {len(data) if data else 0}")

        if data:
            # Convert to timestamps
            timestamps = [ts for ts, _ in data]
            min_ts = min(timestamps)
            max_ts = max(timestamps)

            min_dt = datetime.fromtimestamp(min_ts/1000, tz=timezone.utc)
            max_dt = datetime.fromtimestamp(max_ts/1000, tz=timezone.utc)

            print(f"First data point: {min_dt}")
            print(f"Last data point: {max_dt}")

            # Check for gaps (5 min intervals)
            expected_interval = 5 * 60 * 1000  # 5 minutes in ms
            gaps = []

            for i in range(1, len(timestamps)):
                diff = timestamps[i] - timestamps[i-1]
                if diff > expected_interval + 1000:  # Allow 1 second tolerance
                    gap_start = datetime.fromtimestamp(timestamps[i-1]/1000, tz=timezone.utc)
                    gap_end = datetime.fromtimestamp(timestamps[i]/1000, tz=timezone.utc)
                    gaps.append((gap_start, gap_end, diff / (60 * 1000)))  # diff in minutes

            if gaps:
                print(f"\nGaps found: {len(gaps)}")
                for start, end, duration in gaps:
                    print(f"  Gap: {start} to {end} ({duration:.0f} minutes)")
            else:
                print("\nNo gaps in data!")

            # Check how much data is missing from market open
            if min_dt > market_open_utc:
                missing_minutes = (min_dt - market_open_utc).total_seconds() / 60
                print(f"\n⚠️ Missing data from market open to first data point: {missing_minutes:.0f} minutes")
                print(f"   Should have data starting at {market_open_utc}")
                print(f"   But first data point is at {min_dt}")
        else:
            print("No data found in Redis!")

    except Exception as e:
        print(f"Error checking Redis: {e}")

    await redis.aclose()


async def check_db_data():
    """Check what data is in DB for NIFTY"""
    print("\n" + "=" * 60)
    print("DATABASE DATA FOR NIFTY")
    print("=" * 60)

    async for session in get_db_session():
        # Get NIFTY instrument ID
        result = await session.execute(
            select(Instrument).where(Instrument.symbol == "NIFTY")
        )
        instrument = result.scalar_one_or_none()

        if not instrument:
            print("NIFTY instrument not found in database!")
            return

        print(f"NIFTY instrument_id: {instrument.id}")

        # Get today's data
        today_start = datetime(2026, 1, 20, 0, 0, 0, tzinfo=timezone.utc)
        today_end = today_start + timedelta(days=1)

        result = await session.execute(
            select(
                func.count(PriceHistoryIntraday.id).label("count"),
                func.min(PriceHistoryIntraday.datetime).label("min_dt"),
                func.max(PriceHistoryIntraday.datetime).label("max_dt"),
            ).where(
                PriceHistoryIntraday.instrument_id == instrument.id,
                PriceHistoryIntraday.datetime >= today_start,
                PriceHistoryIntraday.datetime < today_end
            )
        )
        row = result.one()

        print(f"\nToday's records in DB:")
        print(f"  Total records: {row.count}")
        print(f"  First record: {row.min_dt}")
        print(f"  Last record: {row.max_dt}")

        # Get sample of records that need resolution
        result = await session.execute(
            select(PriceHistoryIntraday).where(
                PriceHistoryIntraday.instrument_id == instrument.id,
                PriceHistoryIntraday.datetime >= today_start,
                PriceHistoryIntraday.datetime < today_end,
                PriceHistoryIntraday.resolve_required == True
            ).order_by(PriceHistoryIntraday.datetime).limit(10)
        )
        records = result.scalars().all()

        if records:
            print("\nSample records needing resolution:")
            for r in records:
                print(f"  {r.datetime}: O={r.open} H={r.high} L={r.low} C={r.close}")

        break


async def main():
    await check_redis_data()
    await check_db_data()


if __name__ == "__main__":
    asyncio.run(main())

