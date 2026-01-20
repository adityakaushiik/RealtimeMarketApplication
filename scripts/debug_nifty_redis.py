"""
Debug script to check Redis data for NIFTY across different time ranges.
Run this to understand the timestamp situation.
"""
import asyncio
import sys
import os
from datetime import datetime, timezone, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytz
from services.redis_timeseries import get_redis_timeseries


async def main():
    print("=" * 80)
    print("NIFTY Redis Data Debug")
    print("=" * 80)

    ist_tz = pytz.timezone("Asia/Kolkata")
    rts = get_redis_timeseries()
    r = rts._get_client()  # Access the internal Redis client

    # First check what keys exist
    print("\n[1] Checking available NIFTY keys in Redis...")
    keys = await r.keys("NIFTY:*")
    for k in keys:
        key_str = k.decode() if isinstance(k, bytes) else str(k)
        print(f"  - {key_str}")

    # Get today's date
    now_utc = datetime.now(timezone.utc)
    today_start_utc = now_utc.replace(hour=0, minute=0, second=0, microsecond=0)
    yesterday_start_utc = today_start_utc - timedelta(days=1)

    # Define ranges to check
    ranges = [
        ("Yesterday UTC 00:00 to Midnight", yesterday_start_utc, today_start_utc),
        ("Today UTC 00:00 to now", today_start_utc, now_utc),
        ("NSE Open IST (03:45 UTC) to now", today_start_utc.replace(hour=3, minute=45), now_utc),
        ("Wrong timezone range (09:00-16:00 UTC)", today_start_utc.replace(hour=9, minute=0), today_start_utc.replace(hour=16, minute=0)),
    ]

    print("\n[2] Checking 5m candle data in different time ranges...")

    for name, start, end in ranges:
        start_ms = int(start.timestamp() * 1000)
        end_ms = int(end.timestamp() * 1000)

        candles = await rts.get_5m_candles("NIFTY", from_ts=start_ms, to_ts=end_ms)
        count = len(candles.get("timestamp", []))

        print(f"\n{name}:")
        print(f"  Range: {start.strftime('%Y-%m-%d %H:%M UTC')} to {end.strftime('%Y-%m-%d %H:%M UTC')}")
        print(f"  Found: {count} candles")

        if count > 0 and count < 15:
            timestamps = candles["timestamp"]
            for ts in timestamps[:5]:
                dt_utc = datetime.fromtimestamp(ts / 1000, tz=timezone.utc)
                dt_ist = dt_utc.astimezone(ist_tz)
                print(f"    {ts} -> {dt_utc.strftime('%H:%M UTC')} = {dt_ist.strftime('%H:%M IST')}")
            if len(timestamps) > 5:
                print("    ...")

    # Check the 5m:close key info
    print("\n[3] Checking 5m series info...")
    for field in ["open", "high", "low", "close", "volume"]:
        key = f"NIFTY:5m:{field}"
        try:
            info = await r.ts().info(key)
            print(f"  {key}:")
            print(f"    First: {info.first_timestamp} ({datetime.fromtimestamp(info.first_timestamp/1000, tz=timezone.utc).strftime('%Y-%m-%d %H:%M UTC') if info.first_timestamp else 'N/A'})")
            print(f"    Last:  {info.last_timestamp} ({datetime.fromtimestamp(info.last_timestamp/1000, tz=timezone.utc).strftime('%Y-%m-%d %H:%M UTC') if info.last_timestamp else 'N/A'})")
            print(f"    Count: {info.total_samples}")
        except Exception as e:
            print(f"  {key}: Error - {e}")

    print("\n[4] ANALYSIS:")
    print("=" * 80)

    # Get all data to analyze
    all_candles = await rts.get_5m_candles("NIFTY", from_ts=int(yesterday_start_utc.timestamp()*1000), to_ts=int(now_utc.timestamp()*1000))
    if all_candles.get("timestamp"):
        timestamps = all_candles["timestamp"]

        # Find morning session data (should be 03:45-10:00 UTC for IST 09:15-15:30)
        morning_correct = [ts for ts in timestamps if 3 <= datetime.fromtimestamp(ts/1000, tz=timezone.utc).hour <= 10]
        morning_wrong = [ts for ts in timestamps if 9 <= datetime.fromtimestamp(ts/1000, tz=timezone.utc).hour <= 16
                        and datetime.fromtimestamp(ts/1000, tz=timezone.utc).date() == now_utc.date()]

        print(f"\nCandles at CORRECT UTC hours (03:45-10:00): {len(morning_correct)}")
        print(f"Candles at WRONG UTC hours (09:00-16:00 today): {len(morning_wrong)}")

        if len(morning_wrong) > len(morning_correct):
            print("\n[WARNING] There appears to be data stored with IST timestamps as UTC!")
            print("You may need to clear Redis and refetch with the fixed code.")
        else:
            print("\n[OK] Timestamps appear to be correct UTC values.")

    print("\nDone.")


if __name__ == "__main__":
    asyncio.run(main())

