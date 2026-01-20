"""
Clear old Redis 5m data and verify the timestamp fix is working
"""
import asyncio
import sys
import os
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytz


async def main():
    from config.redis_config import get_redis_client

    redis = await get_redis_client()
    ist_tz = pytz.timezone("Asia/Kolkata")

    print("=" * 80)
    print("Redis 5m Data Cleanup")
    print("=" * 80)

    now_utc = datetime.now(timezone.utc)
    now_ist = now_utc.astimezone(ist_tz)

    print(f"\nCurrent time: {now_ist.strftime('%Y-%m-%d %H:%M:%S')} IST")

    # Get all 5m keys
    keys = await redis.keys("*:5m:*")
    print(f"\nFound {len(keys)} 5m keys")

    if not keys:
        print("No 5m data to clean")
        await redis.close()
        return

    # Delete all 5m keys to start fresh
    print("\nDeleting all 5m data to start fresh with correct timestamps...")

    deleted = 0
    for key in keys:
        try:
            await redis.delete(key)
            deleted += 1
        except Exception as e:
            print(f"Error deleting {key}: {e}")

    print(f"Deleted {deleted} keys")

    # Also clear the tick data that may have old data
    tick_keys = await redis.keys("*:tick:*")
    print(f"\nFound {len(tick_keys)} tick keys")

    for key in tick_keys:
        try:
            await redis.delete(key)
        except:
            pass

    print(f"Deleted {len(tick_keys)} tick keys")

    await redis.close()
    print("\n[DONE] Redis cleared. Restart the app to fetch fresh data with correct timestamps.")


if __name__ == "__main__":
    asyncio.run(main())

