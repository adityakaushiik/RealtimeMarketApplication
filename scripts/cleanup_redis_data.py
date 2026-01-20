"""
Clean up incorrect Redis data and verify the fix
"""
import asyncio
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from datetime import datetime, timezone
import pytz


async def main():
    from config.redis_config import get_redis_client

    redis = await get_redis_client()
    ist_tz = pytz.timezone("Asia/Kolkata")

    print("=" * 80)
    print("Redis Cleanup and Verification")
    print("=" * 80)

    now_utc = datetime.now(timezone.utc)
    now_ist = now_utc.astimezone(ist_tz)
    now_ms = int(now_utc.timestamp() * 1000)

    print(f"\nCurrent time: {now_ist.strftime('%Y-%m-%d %H:%M:%S')} IST")
    print(f"Current epoch (ms): {now_ms}")

    # Check NIFTY data
    test_keys = [
        "NIFTY:5m:open",
        "NIFTY:5m:close",
        "NIFTY:5m:high",
        "NIFTY:5m:low",
        "NIFTY:5m:volume"
    ]

    print("\n" + "-" * 80)
    print("Current Redis 5m data for NIFTY:")
    print("-" * 80)

    for key in test_keys[:1]:  # Just check open
        try:
            info = await redis.execute_command("TS.INFO", key)
            info_dict = dict(zip(info[::2], info[1::2]))
            first_ts = info_dict.get(b'firstTimestamp', 0)
            last_ts = info_dict.get(b'lastTimestamp', 0)

            first_dt = datetime.fromtimestamp(first_ts / 1000, tz=timezone.utc)
            last_dt = datetime.fromtimestamp(last_ts / 1000, tz=timezone.utc)

            print(f"\n{key}:")
            print(f"  First: {first_ts} ({first_dt.strftime('%Y-%m-%d %H:%M')} UTC)")
            print(f"  Last:  {last_ts} ({last_dt.strftime('%Y-%m-%d %H:%M')} UTC)")

            # Check if data is in the future
            if first_ts > now_ms:
                print(f"  [ERROR] Data is in the FUTURE! ({(first_ts - now_ms) / 3600000:.1f} hours ahead)")
                print(f"  Deleting corrupt future data...")

                # Delete all future data
                await redis.execute_command("TS.DEL", key, now_ms, first_ts + 86400000)
                print(f"  [DONE] Deleted future data")

        except Exception as e:
            print(f"  Error: {e}")

    # Now check again
    print("\n" + "-" * 80)
    print("Verifying cleanup:")
    print("-" * 80)

    for key in test_keys[:1]:
        try:
            info = await redis.execute_command("TS.INFO", key)
            info_dict = dict(zip(info[::2], info[1::2]))
            count = info_dict.get(b'totalSamples', 0)
            print(f"{key}: {count} samples remaining")
        except Exception as e:
            print(f"{key}: {e}")

    await redis.close()
    print("\n[DONE] Cleanup complete. Restart the app to fetch fresh data.")


if __name__ == "__main__":
    asyncio.run(main())

