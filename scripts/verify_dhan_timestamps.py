"""
Verify Dhan timestamp handling - both WebSocket and Historical API.
Check if IST timestamps are being incorrectly treated as UTC.
"""
import asyncio
import sys
import os
from datetime import datetime, timezone, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytz
from config.redis_config import get_redis


async def main():
    print("=" * 80)
    print("Dhan Timestamp Verification")
    print("=" * 80)

    ist_tz = pytz.timezone("Asia/Kolkata")
    now_utc = datetime.now(timezone.utc)
    now_ist = now_utc.astimezone(ist_tz)

    print(f"\nCurrent time:")
    print(f"  UTC: {now_utc.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    print(f"  IST: {now_ist.strftime('%Y-%m-%d %H:%M:%S %Z')}")

    redis = get_redis()

    # 1. Check Redis TimeSeries data for NIFTY
    print("\n" + "=" * 60)
    print("1. Checking Redis TimeSeries for NIFTY")
    print("=" * 60)

    # Get the last few 5m candles from Redis
    try:
        # Query last 2 hours of data
        from_ts = int((now_utc - timedelta(hours=2)).timestamp() * 1000)
        to_ts = int(now_utc.timestamp() * 1000)

        open_key = "NIFTY:5m:open"
        data = await redis.execute_command("TS.RANGE", open_key, from_ts, to_ts)

        if data:
            print(f"\nFound {len(data)} 5m candles in Redis for NIFTY")
            print(f"\nTimestamp Analysis:")
            print("-" * 80)
            print(f"{'Raw TS (ms)':<18} {'As UTC':<28} {'As IST (if stored as UTC)':<28}")
            print("-" * 80)

            for ts_ms, val in data[-10:]:  # Last 10 entries
                # Interpret as UTC timestamp
                dt_utc = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
                dt_ist = dt_utc.astimezone(ist_tz)

                print(f"{ts_ms:<18} {dt_utc.strftime('%Y-%m-%d %H:%M:%S %Z'):<28} {dt_ist.strftime('%Y-%m-%d %H:%M:%S %Z'):<28}")

            # Analysis
            if data:
                latest_ts = data[-1][0]
                latest_utc = datetime.fromtimestamp(latest_ts / 1000, tz=timezone.utc)
                latest_ist = latest_utc.astimezone(ist_tz)

                print("\n📊 Analysis:")
                print(f"   Latest timestamp in Redis: {latest_ts}")
                print(f"   Interpreted as UTC: {latest_utc.strftime('%Y-%m-%d %H:%M:%S')}")
                print(f"   Converted to IST: {latest_ist.strftime('%Y-%m-%d %H:%M:%S')}")

                # NSE trades from 9:15 IST to 15:30 IST
                # If timestamps are correctly in UTC:
                #   - 9:15 IST = 3:45 UTC
                #   - 15:30 IST = 10:00 UTC
                # So UTC hours should be in range 3-10

                utc_hour = latest_utc.hour
                if 3 <= utc_hour <= 10:
                    print(f"   ✅ Hour {utc_hour} UTC is within expected NSE trading hours (3:45-10:00 UTC)")
                    print(f"      This means timestamps are correctly stored as UTC")
                elif 9 <= utc_hour <= 16:
                    print(f"   ⚠️ Hour {utc_hour} UTC looks like IST trading hours!")
                    print(f"      This means IST timestamps might be stored as UTC incorrectly!")
                    print(f"      The data is shifted by 5.5 hours!")
        else:
            print("   No 5m candle data found in Redis for NIFTY")

    except Exception as e:
        print(f"   Error reading Redis: {e}")

    # 2. Check tick data in Redis
    print("\n" + "=" * 60)
    print("2. Checking Redis Tick Data for NIFTY")
    print("=" * 60)

    try:
        tick_key = "NIFTY:tick:price"
        # Get last 10 ticks
        data = await redis.execute_command("TS.RANGE", tick_key, "-", "+", "COUNT", 10)

        if data:
            print(f"\nFound tick data for NIFTY")
            print(f"\nTimestamp Analysis:")
            print("-" * 80)
            print(f"{'Raw TS (ms)':<18} {'As UTC':<28} {'As IST (if stored as UTC)':<28}")
            print("-" * 80)

            for ts_ms, val in data:
                dt_utc = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
                dt_ist = dt_utc.astimezone(ist_tz)
                print(f"{ts_ms:<18} {dt_utc.strftime('%Y-%m-%d %H:%M:%S %Z'):<28} {dt_ist.strftime('%Y-%m-%d %H:%M:%S %Z'):<28}")
        else:
            print("   No tick data found")
    except Exception as e:
        print(f"   Error: {e}")

    # 3. Verify what Dhan WebSocket sends
    print("\n" + "=" * 60)
    print("3. Dhan WebSocket Timestamp Handling")
    print("=" * 60)

    print("""
According to Dhan documentation and your code:

WebSocket Feed (dhan_provider.py):
- Dhan sends 'LTT' (Last Traded Time) as a timestamp
- Your code does: datetime.fromtimestamp(ltt, tz=timezone.utc)
- This assumes Dhan sends UTC epoch seconds

BUT Dhan actually sends IST timestamps!
- If Dhan sends 1737357600 (which represents 2025-01-20 15:30:00 IST)
- Your code interprets it as 2025-01-20 15:30:00 UTC
- This is WRONG - it's 5.5 hours ahead!

Historical API (get_intraday_prices):
- Comment says: "Dhan Historical API returns valid UTC timestamp. No shift needed."
- But this may not be accurate!
    """)

    # 4. Calculate the expected vs actual timestamps
    print("\n" + "=" * 60)
    print("4. Expected vs Actual Comparison")
    print("=" * 60)

    # NSE opened at 9:15 IST today
    market_open_ist = ist_tz.localize(datetime(2026, 1, 20, 9, 15, 0))
    market_open_utc = market_open_ist.astimezone(timezone.utc)

    print(f"\nNSE Market Open Today:")
    print(f"  IST: {market_open_ist.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    print(f"  UTC: {market_open_utc.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    print(f"  Epoch (ms): {int(market_open_utc.timestamp() * 1000)}")

    # If Dhan sends IST as epoch (common mistake)
    # They might send the epoch of 9:15 as if it were UTC
    wrong_epoch = int(market_open_ist.replace(tzinfo=timezone.utc).timestamp() * 1000)
    print(f"\n  If Dhan incorrectly sends IST as UTC epoch: {wrong_epoch}")
    print(f"  Difference: {wrong_epoch - int(market_open_utc.timestamp() * 1000)} ms = {(wrong_epoch - int(market_open_utc.timestamp() * 1000)) / 1000 / 3600:.1f} hours")

    await redis.aclose()

    print("\n" + "=" * 60)
    print("RECOMMENDATION")
    print("=" * 60)
    print("""
To fix this issue, you need to:

1. Verify if Dhan sends IST or UTC timestamps by:
   - Looking at actual raw timestamp values
   - Comparing them to known market open/close times

2. If Dhan sends IST timestamps:
   - Convert them to UTC before storing
   - In WebSocket: dt = datetime.fromtimestamp(ltt, tz=ist_tz).astimezone(timezone.utc)
   - In Historical API: Similar conversion needed

3. The fact that data "before 1pm IST" is missing suggests:
   - Morning data (9:15-13:00 IST) stored with wrong timezone
   - When querying in UTC, it can't find those records
    """)


if __name__ == "__main__":
    asyncio.run(main())

