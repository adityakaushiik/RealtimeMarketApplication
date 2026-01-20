"""
Verify the timestamp fix is working correctly.
"""
import asyncio
import sys
import os
from datetime import datetime, timezone, timedelta
import aiohttp

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytz
from config.settings import Settings


async def main():
    print("=" * 80)
    print("Verifying Dhan Timestamp Fix")
    print("=" * 80)

    settings = Settings()
    ist_tz = pytz.timezone("Asia/Kolkata")

    now_utc = datetime.now(timezone.utc)
    now_ist = now_utc.astimezone(ist_tz)

    print(f"\nCurrent time:")
    print(f"  UTC: {now_utc.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    print(f"  IST: {now_ist.strftime('%Y-%m-%d %H:%M:%S %Z')}")

    # Format dates for Dhan API (IST format)
    from_date = (now_ist - timedelta(hours=6)).strftime("%Y-%m-%d %H:%M:%S")
    to_date = now_ist.strftime("%Y-%m-%d %H:%M:%S")

    # NIFTY 50 - security ID for Index
    payload = {
        "securityId": "13",
        "exchangeSegment": "IDX_I",
        "instrument": "INDEX",
        "interval": "5",
        "oi": False,
        "fromDate": from_date,
        "toDate": to_date,
    }

    url = "https://api.dhan.co/v2/charts/intraday"
    headers = {
        "access-token": settings.DHAN_ACCESS_TOKEN,
        "Content-Type": "application/json",
    }

    async with aiohttp.ClientSession() as session:
        async with session.post(url, json=payload, headers=headers) as response:
            if response.status != 200:
                print(f"\nError: {response.status}")
                return
            data = await response.json()

    if "timestamp" not in data:
        print(f"\nNo timestamp data returned!")
        return

    timestamps = data.get("timestamp", [])
    opens = data.get("open", [])

    print(f"\n✅ Received {len(timestamps)} candles from Dhan API")

    print("\n" + "=" * 80)
    print("BEFORE FIX (treating as UTC directly - WRONG)")
    print("=" * 80)
    print(f"{'Raw Epoch':<14} {'As UTC (WRONG)':<24} {'IST (WRONG)':<24} {'Open':<12}")
    print("-" * 80)

    for i, ts in enumerate(timestamps[:5]):  # First 5 candles
        # OLD CODE (wrong)
        dt_wrong = datetime.fromtimestamp(ts, tz=timezone.utc)
        dt_wrong_ist = dt_wrong.astimezone(ist_tz)
        print(f"{ts:<14} {dt_wrong.strftime('%Y-%m-%d %H:%M:%S'):<24} {dt_wrong_ist.strftime('%Y-%m-%d %H:%M:%S'):<24} {opens[i]:<12.2f}")

    print("\n" + "=" * 80)
    print("AFTER FIX (converting IST epoch to UTC - CORRECT)")
    print("=" * 80)
    print(f"{'Raw Epoch':<14} {'UTC (CORRECT)':<24} {'IST (CORRECT)':<24} {'Open':<12}")
    print("-" * 80)

    for i, ts in enumerate(timestamps[:5]):  # First 5 candles
        # NEW CODE (correct) - subtract 5.5 hours (19800 seconds)
        ts_utc = ts - 19800
        dt_correct = datetime.fromtimestamp(ts_utc, tz=timezone.utc)
        dt_correct_ist = dt_correct.astimezone(ist_tz)
        print(f"{ts:<14} {dt_correct.strftime('%Y-%m-%d %H:%M:%S'):<24} {dt_correct_ist.strftime('%Y-%m-%d %H:%M:%S'):<24} {opens[i]:<12.2f}")

    print("\n" + "=" * 80)
    print("VERIFICATION")
    print("=" * 80)

    # First candle should be at 9:15 IST (market open) = 03:45 UTC
    if timestamps:
        first_ts = timestamps[0]
        ts_utc = first_ts - 19800
        dt_correct = datetime.fromtimestamp(ts_utc, tz=timezone.utc)
        dt_correct_ist = dt_correct.astimezone(ist_tz)

        print(f"\nFirst candle:")
        print(f"  UTC time: {dt_correct.strftime('%H:%M:%S')} (should be ~03:45 for market open)")
        print(f"  IST time: {dt_correct_ist.strftime('%H:%M:%S')} (should be ~09:15 for market open)")

        if 3 <= dt_correct.hour <= 4 and dt_correct_ist.hour == 9:
            print(f"\n✅ FIX VERIFIED! Timestamps are now correctly in UTC")
        else:
            print(f"\n⚠️ Still needs verification")


if __name__ == "__main__":
    asyncio.run(main())

