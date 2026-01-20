"""
Check what timestamps are stored in Redis for NIFTY
"""
import asyncio
import sys
import os
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytz
from services.redis_timeseries import get_redis_timeseries


async def main():
    print("=" * 80)
    print("Redis TimeSeries Data Check for NIFTY")
    print("=" * 80)

    ist_tz = pytz.timezone("Asia/Kolkata")
    rts = get_redis_timeseries()

    # Get today's data
    now = datetime.now(timezone.utc)
    today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)

    today_start_ms = int(today_start.timestamp() * 1000)
    now_ms = int(now.timestamp() * 1000)

    print(f"\nCurrent time UTC: {now}")
    print(f"Today start UTC: {today_start}")
    print(f"Query range: {today_start_ms} to {now_ms}")

    try:
        candles = await rts.get_all_intraday_candles(
            symbol="NIFTY",
            from_ts=today_start_ms,
            to_ts=now_ms,
        )

        if not candles or not candles.get("timestamp"):
            print("\n[X] No data found in Redis for NIFTY!")
            # Check what keys exist
            redis_client = rts.redis
            keys = await redis_client.keys("NIFTY:*")
            print(f"Available keys for NIFTY: {keys}")
            return

        timestamps = candles["timestamp"]
        opens = candles["open"]
        closes = candles["close"]

        print(f"\n[OK] Found {len(timestamps)} candles in Redis")
        print("\n" + "=" * 80)
        print(f"{'Timestamp (ms)':<18} {'UTC Time':<24} {'IST Time':<24} {'Open':<12} {'Close':<12}")
        print("-" * 100)

        # Show first 10 and last 5 candles
        for i, ts in enumerate(timestamps):
            if i < 10 or i >= len(timestamps) - 5:
                dt_utc = datetime.fromtimestamp(ts / 1000, tz=timezone.utc)
                dt_ist = dt_utc.astimezone(ist_tz)
                open_price = opens[i] if i < len(opens) else "N/A"
                close_price = closes[i] if i < len(closes) else "N/A"
                print(f"{ts:<18} {dt_utc.strftime('%Y-%m-%d %H:%M:%S'):<24} {dt_ist.strftime('%Y-%m-%d %H:%M:%S'):<24} {open_price:<12.2f} {close_price:<12.2f}")
            elif i == 10:
                print("...")

        # Check if timestamps are in expected range
        print("\n" + "=" * 80)
        print("ANALYSIS")
        print("=" * 80)

        first_ts = timestamps[0]
        first_dt_utc = datetime.fromtimestamp(first_ts / 1000, tz=timezone.utc)
        first_dt_ist = first_dt_utc.astimezone(ist_tz)

        print(f"\nFirst candle: {first_dt_utc.strftime('%H:%M:%S UTC')} = {first_dt_ist.strftime('%H:%M:%S IST')}")

        # NSE opens at 9:15 IST = 03:45 UTC
        if 3 <= first_dt_utc.hour <= 4 and first_dt_ist.hour == 9:
            print("[OK] Timestamps appear CORRECT (9:15 IST = ~03:45 UTC)")
        elif 9 <= first_dt_utc.hour <= 10:
            print("[WRONG] Timestamps are WRONG - IST stored as UTC!")
            print("   First candle shows ~09:15 UTC which should be 09:15 IST")
            print("   You need to clear Redis and refetch data with the fixed code!")
        else:
            print(f"[WARN] Unexpected time range - first candle at {first_dt_utc.strftime('%H:%M UTC')}")

    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    asyncio.run(main())

