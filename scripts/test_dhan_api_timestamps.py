"""
Make a direct API call to Dhan to verify timestamp format.
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
    print("Direct Dhan API Timestamp Verification")
    print("=" * 80)

    settings = Settings()
    ist_tz = pytz.timezone("Asia/Kolkata")

    # Use Dhan API to fetch NIFTY data
    now_utc = datetime.now(timezone.utc)
    now_ist = now_utc.astimezone(ist_tz)

    print(f"\nCurrent time:")
    print(f"  UTC: {now_utc.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    print(f"  IST: {now_ist.strftime('%Y-%m-%d %H:%M:%S %Z')}")

    # Format dates for Dhan API (IST format)
    from_date = (now_ist - timedelta(hours=2)).strftime("%Y-%m-%d %H:%M:%S")
    to_date = now_ist.strftime("%Y-%m-%d %H:%M:%S")

    print(f"\nRequesting data from Dhan:")
    print(f"  From: {from_date} (IST)")
    print(f"  To: {to_date} (IST)")

    # NIFTY 50 - security ID for Index
    payload = {
        "securityId": "13",  # NIFTY 50 security ID on NSE
        "exchangeSegment": "IDX_I",  # Index segment
        "instrument": "INDEX",
        "interval": "5",
        "oi": False,
        "fromDate": from_date,
        "toDate": to_date,
    }

    print(f"\nPayload: {payload}")

    url = "https://api.dhan.co/v2/charts/intraday"
    headers = {
        "access-token": settings.DHAN_ACCESS_TOKEN,
        "Content-Type": "application/json",
    }

    async with aiohttp.ClientSession() as session:
        async with session.post(url, json=payload, headers=headers) as response:
            if response.status != 200:
                print(f"\nError: {response.status}")
                text = await response.text()
                print(f"Response: {text}")
                return

            data = await response.json()

    if "timestamp" not in data:
        print(f"\nNo timestamp data returned!")
        print(f"Response: {data}")
        return

    timestamps = data.get("timestamp", [])
    opens = data.get("open", [])

    print(f"\n[OK] Received {len(timestamps)} candles from Dhan API")
    print("\n" + "=" * 80)
    print("TIMESTAMP ANALYSIS")
    print("=" * 80)

    print(f"\n{'Raw Epoch':<14} {'As UTC (if epoch is UTC)':<28} {'As IST (if epoch is IST)':<28} {'Open Price':<12}")
    print("-" * 100)

    for i, ts in enumerate(timestamps[-10:]):  # Last 10
        # Interpretation 1: Epoch is UTC
        dt_if_utc = datetime.fromtimestamp(ts, tz=timezone.utc)

        # Interpretation 2: Epoch represents IST time but stored without TZ
        # This means we need to interpret the epoch as if it's IST
        dt_if_ist = datetime.fromtimestamp(ts, tz=ist_tz)

        # What the API might actually mean - the epoch represents IST local time
        # So we should interpret it as: the timestamp IS already in IST
        naive_dt = datetime.fromtimestamp(ts)  # System local time

        print(f"{ts:<14} {dt_if_utc.strftime('%Y-%m-%d %H:%M:%S %Z'):<28} {dt_if_ist.strftime('%Y-%m-%d %H:%M:%S %Z'):<28} {opens[i] if i < len(opens) else 'N/A':<12}")

    print("\n" + "=" * 80)
    print("VERIFICATION")
    print("=" * 80)

    if timestamps:
        # Market opens at 9:15 IST and closes at 15:30 IST
        # Let's check if the first timestamp of the day makes sense
        first_ts = timestamps[0]
        last_ts = timestamps[-1]

        # If epoch is UTC
        first_as_utc = datetime.fromtimestamp(first_ts, tz=timezone.utc)
        last_as_utc = datetime.fromtimestamp(last_ts, tz=timezone.utc)

        # Convert to IST for display
        first_as_ist = first_as_utc.astimezone(ist_tz)
        last_as_ist = last_as_utc.astimezone(ist_tz)

        print(f"\nFirst candle: epoch={first_ts}")
        print(f"  If treated as UTC epoch → {first_as_utc.strftime('%H:%M IST')}")
        print(f"  This means IST time would be: {first_as_ist.strftime('%H:%M IST')}")

        print(f"\nLast candle: epoch={last_ts}")
        print(f"  If treated as UTC epoch → {last_as_utc.strftime('%H:%M UTC')}")
        print(f"  This means IST time would be: {last_as_ist.strftime('%H:%M IST')}")

        # NSE market hours: 9:15-15:30 IST = 3:45-10:00 UTC
        print(f"\n[RESULT] CONCLUSION:")

        first_utc_hour = first_as_utc.hour
        last_utc_hour = last_as_utc.hour

        # If first candle is around 3:45-4:00 UTC, it's correctly stored as UTC
        # If first candle is around 9:15-9:30 UTC, it's actually IST stored incorrectly

        if 3 <= first_utc_hour <= 5:
            print(f"   [CORRECT] Timestamps appear to be CORRECT UTC epochs")
            print(f"   First candle at {first_utc_hour}:{first_as_utc.minute:02d} UTC = {first_as_ist.hour}:{first_as_ist.minute:02d} IST (market open)")
        elif 9 <= first_utc_hour <= 10:
            print(f"   [WARNING] Timestamps are IST but stored as if they were UTC!")
            print(f"   First candle shows {first_utc_hour}:{first_as_utc.minute:02d} UTC")
            print(f"   But this is actually {first_utc_hour}:{first_as_utc.minute:02d} IST (market open)")
            print(f"\n   FIX REQUIRED: Interpret epoch as IST, then convert to UTC")
            print(f"   dt = datetime.fromtimestamp(ts, tz=ist_tz).astimezone(timezone.utc)")


if __name__ == "__main__":
    asyncio.run(main())

