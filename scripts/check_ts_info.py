"""Check Redis TS info"""
import asyncio
import sys
import os
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.redis_config import get_redis


async def main():
    redis = get_redis()

    # Check TS.INFO for NIFTY:5m:open
    try:
        result = await redis.execute_command("TS.INFO", "NIFTY:5m:open")
        print("Raw result type:", type(result))
        print("Raw result:", result)

        # Parse the result (it's a list of key-value pairs)
        info = {}
        if isinstance(result, list):
            for i in range(0, len(result), 2):
                key = result[i].decode() if isinstance(result[i], bytes) else str(result[i])
                info[key] = result[i+1]
        else:
            # It might be a TSInfo object
            info = {
                'retentionTime': getattr(result, 'retention_msecs', 0),
                'totalSamples': getattr(result, 'total_samples', 0),
                'firstTimestamp': getattr(result, 'first_timestamp', 0),
                'lastTimestamp': getattr(result, 'last_timestamp', 0),
            }

        print("\nNIFTY:5m:open Time Series Info:")
        retention = info.get('retentionTime') or info.get('retention_msecs', 0)
        print(f"  Retention: {retention} ms ({retention/1000/60/60:.1f} hours)")
        print(f"  Total samples: {info.get('totalSamples', info.get('total_samples', 'N/A'))}")

        first_ts = info.get('firstTimestamp') or info.get('first_timestamp', 0)
        last_ts = info.get('lastTimestamp') or info.get('last_timestamp', 0)

        if first_ts:
            first_dt = datetime.fromtimestamp(first_ts/1000, tz=timezone.utc)
            print(f"  First timestamp: {first_dt} ({first_ts})")
        if last_ts:
            last_dt = datetime.fromtimestamp(last_ts/1000, tz=timezone.utc)
            print(f"  Last timestamp: {last_dt} ({last_ts})")

        # Calculate the oldest timestamp that can be inserted
        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        retention_ms = info.get('retentionTime', 0)
        oldest_allowed = now_ms - retention_ms
        oldest_allowed_dt = datetime.fromtimestamp(oldest_allowed/1000, tz=timezone.utc)
        print(f"\n  Current time (UTC): {datetime.now(timezone.utc)}")
        print(f"  Oldest insertable timestamp: {oldest_allowed_dt}")

        # Market open is 03:45 UTC
        market_open = datetime(2026, 1, 20, 3, 45, 0, tzinfo=timezone.utc)
        market_open_ms = int(market_open.timestamp() * 1000)
        print(f"\n  Market open (03:45 UTC): {market_open}")
        print(f"  Market open timestamp: {market_open_ms}")

        if market_open_ms < oldest_allowed:
            print(f"\n  ⚠️ Market open is OLDER than oldest insertable - data cannot be backfilled!")
            diff_hours = (oldest_allowed - market_open_ms) / 1000 / 60 / 60
            print(f"     Gap: {diff_hours:.1f} hours")
        else:
            print(f"\n  ✅ Market open is within retention window - should be insertable")

    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()

    await redis.aclose()


if __name__ == "__main__":
    asyncio.run(main())

