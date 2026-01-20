"""Debug Redis TimeSeries operations"""
import asyncio
import sys
import os
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from services.redis_timeseries import get_redis_timeseries
from config.redis_config import get_redis


async def test_add_5m_candle():
    """Test adding a 5m candle directly"""
    rts = get_redis_timeseries()
    redis = get_redis()

    # Test with a known timestamp
    test_time = datetime(2026, 1, 20, 4, 0, 0, tzinfo=timezone.utc)  # 9:30 IST
    timestamp_ms = int(test_time.timestamp() * 1000)

    print(f"Testing add_5m_candle for NIFTY at {test_time}")
    print(f"Timestamp (ms): {timestamp_ms}")

    try:
        await rts.add_5m_candle(
            symbol="NIFTY",
            timestamp_ms=timestamp_ms,
            open_price=25500.0,
            high_price=25550.0,
            low_price=25450.0,
            close_price=25525.0,
            volume=1000000,
        )
        print("Successfully added candle")
    except Exception as e:
        print(f"Error adding candle: {e}")
        import traceback
        traceback.print_exc()

    # Now check if it was added
    try:
        data = await redis.execute_command(
            "TS.RANGE",
            "NIFTY:5m:close",
            timestamp_ms - 1000,
            timestamp_ms + 1000
        )
        print(f"\nData around {test_time}:")
        if data:
            for ts, val in data:
                dt = datetime.fromtimestamp(ts/1000, tz=timezone.utc)
                print(f"  {dt}: {val}")
        else:
            print("  No data found!")

        # Check what data exists
        print("\nAll keys for NIFTY:")
        keys = await redis.keys("NIFTY:5m:*")
        for k in keys:
            print(f"  {k}")

        # Check if the time series exists
        try:
            info = await redis.execute_command("TS.INFO", "NIFTY:5m:close")
            print(f"\nNIFTY:5m:close info:")
            for i in range(0, len(info), 2):
                print(f"  {info[i]}: {info[i+1]}")
        except Exception as e:
            print(f"Could not get info: {e}")

    except Exception as e:
        print(f"Error checking data: {e}")
        import traceback
        traceback.print_exc()

    await redis.aclose()


if __name__ == "__main__":
    asyncio.run(test_add_5m_candle())

