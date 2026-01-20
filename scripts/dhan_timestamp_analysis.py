"""
Test to understand Dhan timestamp format
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
    print("Dhan Timestamp Analysis")
    print("=" * 80)

    settings = Settings()
    ist_tz = pytz.timezone("Asia/Kolkata")

    now_utc = datetime.now(timezone.utc)
    now_ist = now_utc.astimezone(ist_tz)

    print(f"\nCurrent time:")
    print(f"  UTC: {now_utc.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  IST: {now_ist.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  UTC epoch: {int(now_utc.timestamp())}")

    # Request data for today's morning session (9:15-9:30 IST)
    # This is 03:45-04:00 UTC
    morning_ist = now_ist.replace(hour=9, minute=15, second=0, microsecond=0)
    morning_end_ist = now_ist.replace(hour=9, minute=30, second=0, microsecond=0)

    from_date_str = morning_ist.strftime("%Y-%m-%d %H:%M:%S")
    to_date_str = morning_end_ist.strftime("%Y-%m-%d %H:%M:%S")

    payload = {
        "securityId": "13",  # NIFTY
        "exchangeSegment": "IDX_I",
        "instrument": "INDEX",
        "interval": "5",
        "oi": False,
        "fromDate": from_date_str,
        "toDate": to_date_str,
    }

    print(f"\nRequest for NIFTY morning data:")
    print(f"  fromDate: {from_date_str} (IST)")
    print(f"  toDate: {to_date_str} (IST)")
    print(f"\nExpected first candle should represent 09:15 IST = 03:45 UTC")

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
        print("\nNo data returned - market may be closed or no data for this period")
        return

    print(f"\nReceived {len(timestamps)} candles")
    print("\n" + "=" * 80)
    print("TIMESTAMP ANALYSIS")
    print("=" * 80)

    for i, ts in enumerate(timestamps):
        # Interpretation 1: ts is a standard UTC epoch
        dt_as_utc = datetime.fromtimestamp(ts, tz=timezone.utc)
        dt_as_utc_to_ist = dt_as_utc.astimezone(ist_tz)

        # Interpretation 2: ts is an "IST epoch" (needs -5.5h adjustment)
        ts_adjusted = ts - 19800
        dt_adjusted = datetime.fromtimestamp(ts_adjusted, tz=timezone.utc)
        dt_adjusted_to_ist = dt_adjusted.astimezone(ist_tz)

        print(f"\nCandle {i+1}: epoch={ts}")
        print(f"  If epoch is UTC: {dt_as_utc.strftime('%H:%M')} UTC = {dt_as_utc_to_ist.strftime('%H:%M')} IST")
        print(f"  If epoch is IST (adjusted): {dt_adjusted.strftime('%H:%M')} UTC = {dt_adjusted_to_ist.strftime('%H:%M')} IST")
        print(f"  Open price: {opens[i]}")

    # Conclusion
    print("\n" + "=" * 80)
    print("CONCLUSION")
    print("=" * 80)

    first_ts = timestamps[0]
    dt_utc = datetime.fromtimestamp(first_ts, tz=timezone.utc)
    dt_ist = dt_utc.astimezone(ist_tz)

    print(f"\nWe requested data starting at 09:15 IST (03:45 UTC)")
    print(f"First candle timestamp shows: {dt_utc.strftime('%H:%M')} UTC = {dt_ist.strftime('%H:%M')} IST")

    if dt_ist.hour == 9 and dt_ist.minute == 15:
        print("\n>>> Dhan returns STANDARD UTC epochs - no adjustment needed!")
        print(">>> datetime.fromtimestamp(ts, tz=timezone.utc) is CORRECT")
    elif dt_utc.hour == 9 and dt_utc.minute == 15:
        print("\n>>> Dhan returns IST epochs (IST time stored as if UTC)")
        print(">>> Need to subtract 5.5 hours: ts - 19800")
    else:
        print(f"\n>>> Unexpected time! Need manual investigation.")


if __name__ == "__main__":
    asyncio.run(main())

