"""
Simple test of Dhan API timestamps - ASCII only
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
    print("Dhan API Timestamp Test")
    print("=" * 80)

    settings = Settings()
    ist_tz = pytz.timezone("Asia/Kolkata")

    now_utc = datetime.now(timezone.utc)
    now_ist = now_utc.astimezone(ist_tz)

    print(f"\nCurrent time UTC: {now_utc.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Current time IST: {now_ist.strftime('%Y-%m-%d %H:%M:%S')}")

    # Request last 2 hours of data
    from_date = (now_ist - timedelta(hours=2)).strftime("%Y-%m-%d %H:%M:%S")
    to_date = now_ist.strftime("%Y-%m-%d %H:%M:%S")

    payload = {
        "securityId": "13",
        "exchangeSegment": "IDX_I",
        "instrument": "INDEX",
        "interval": "5",
        "oi": False,
        "fromDate": from_date,
        "toDate": to_date,
    }

    print(f"\nRequest: from={from_date} to={to_date} (IST)")

    url = "https://api.dhan.co/v2/charts/intraday"
    headers = {
        "access-token": settings.DHAN_ACCESS_TOKEN,
        "Content-Type": "application/json",
    }

    async with aiohttp.ClientSession() as session:
        async with session.post(url, json=payload, headers=headers) as response:
            if response.status != 200:
                print(f"Error: {response.status}")
                print(await response.text())
                return
            data = await response.json()

    timestamps = data.get("timestamp", [])
    opens = data.get("open", [])

    if not timestamps:
        print("No data returned!")
        return

    print(f"\nReceived {len(timestamps)} candles")
    print("\n" + "-" * 80)
    print(f"{'Epoch':<15} {'Interpreted as UTC':<25} {'Correct IST':<25}")
    print("-" * 80)

    for i, ts in enumerate(timestamps[-5:]):  # Last 5
        # Standard interpretation: epoch is seconds since 1970-01-01 UTC
        dt_utc = datetime.fromtimestamp(ts, tz=timezone.utc)
        dt_ist = dt_utc.astimezone(ist_tz)
        print(f"{ts:<15} {dt_utc.strftime('%Y-%m-%d %H:%M:%S'):<25} {dt_ist.strftime('%Y-%m-%d %H:%M:%S'):<25}")

    # Analysis
    print("\n" + "=" * 80)
    print("ANALYSIS")
    print("=" * 80)

    first_ts = timestamps[0]
    dt_utc = datetime.fromtimestamp(first_ts, tz=timezone.utc)
    dt_ist = dt_utc.astimezone(ist_tz)

    print(f"\nFirst candle epoch: {first_ts}")
    print(f"  As UTC: {dt_utc.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  As IST: {dt_ist.strftime('%Y-%m-%d %H:%M:%S')}")

    # If market opens at 9:15 IST = 3:45 UTC
    utc_hour = dt_utc.hour
    ist_hour = dt_ist.hour

    if 3 <= utc_hour <= 10:  # Expected UTC hours for IST market
        if 9 <= ist_hour <= 15:  # IST should be 9:15-15:30
            print("\n[OK] Timestamps are CORRECT - epoch values represent UTC")
        else:
            print(f"\n[WARN] Unexpected IST hour: {ist_hour}")
    elif 9 <= utc_hour <= 16:  # This would mean IST times stored as UTC
        print("\n[ERROR] Timestamps are WRONG - IST stored as UTC!")
        print("The epoch values are being calculated from IST time as if it were UTC")
    else:
        print(f"\n[???] Unexpected UTC hour: {utc_hour}")


if __name__ == "__main__":
    asyncio.run(main())

