"""Check Redis data for NIFTY"""
import asyncio
import sys
import os
import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.redis_config import get_redis


async def check_redis_nifty():
    print("Starting Redis check...")
    redis = get_redis()
    print(f"Got redis client: {redis}")

    # Check what keys exist for NIFTY
    keys = await redis.keys("NIFTY:*")
    print(f"NIFTY keys in Redis: {len(keys)}")
    for k in sorted(keys):
        print(f"  {k}")

    # Check 5m candle data
    try:
        # Get all 5m candles for today
        today_start_utc = datetime.datetime(2026, 1, 20, 0, 0, 0, tzinfo=datetime.timezone.utc)
        now_utc = datetime.datetime.now(datetime.timezone.utc)

        from_ts = int(today_start_utc.timestamp() * 1000)
        to_ts = int(now_utc.timestamp() * 1000)

        # Try to get 5m open data
        open_key = "NIFTY:5m:open"
        data = await redis.execute_command("TS.RANGE", open_key, from_ts, to_ts)
        print(f"\nNIFTY:5m:open data points: {len(data) if data else 0}")
        if data:
            for ts, val in data[:10]:
                dt = datetime.datetime.fromtimestamp(ts/1000, tz=datetime.timezone.utc)
                print(f"  {dt}: {val}")
            if len(data) > 10:
                print(f"  ... and {len(data) - 10} more")
    except Exception as e:
        print(f"Error: {e}")

    await redis.aclose()


if __name__ == "__main__":
    asyncio.run(check_redis_nifty())

