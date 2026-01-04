"""Quick test to verify Dhan timestamp format"""
import sys
import asyncio
import struct
from datetime import datetime, timezone
import pytz

sys.path.insert(0, r"C:\Users\tech\OneDrive\Documents\GitHub\RealtimeMarketApplication")

import websockets
from config.settings import get_settings

settings = get_settings()

async def test_timestamp():
    """Connect to Dhan and capture a few ticks to analyze timestamps."""
    url = f"wss://api-feed.dhan.co?version=2&token={settings.DHAN_ACCESS_TOKEN}&clientId={settings.DHAN_CLIENT_ID}&authType=2"

    print("Connecting to Dhan WebSocket...")

    async with websockets.connect(url, ping_interval=30, ping_timeout=10) as ws:
        print("Connected! Subscribing to RELIANCE (2885)...")

        import json
        payload = {
            "RequestCode": 17,
            "InstrumentCount": 1,
            "InstrumentList": [{"ExchangeSegment": "NSE_EQ", "SecurityId": "2885"}]
        }
        await ws.send(json.dumps(payload))
        print("Subscribed. Waiting for ticks...")

        ist_tz = pytz.timezone('Asia/Kolkata')

        tick_count = 0
        while tick_count < 5:
            message = await ws.recv()

            if len(message) < 8:
                continue

            response_code = struct.unpack('<B', message[0:1])[0]

            if response_code == 4:  # Quote
                security_id = struct.unpack('<I', message[4:8])[0]
                ltp = struct.unpack('<f', message[8:12])[0]
                ltq = struct.unpack('<H', message[12:14])[0]
                ltt = struct.unpack('<I', message[14:18])[0]  # Last Traded Time

                tick_count += 1

                # Current time
                now_utc = datetime.now(timezone.utc)
                now_ist = now_utc.astimezone(ist_tz)

                # Interpret timestamp as-is (assuming UTC)
                dt_as_utc = datetime.fromtimestamp(ltt, tz=timezone.utc)

                # Interpret timestamp as IST encoded as Unix
                # (subtract 5.5 hours to get actual UTC)
                dt_adjusted = datetime.fromtimestamp(ltt - 19800, tz=timezone.utc)

                print(f"\n{'='*60}")
                print(f"Tick #{tick_count} for Security {security_id}")
                print(f"  LTP: {ltp:.2f}, LTQ: {ltq}")
                print(f"  Raw LTT (epoch): {ltt}")
                print(f"")
                print(f"  Current Time (UTC):     {now_utc.strftime('%Y-%m-%d %H:%M:%S')}")
                print(f"  Current Time (IST):     {now_ist.strftime('%Y-%m-%d %H:%M:%S')}")
                print(f"")
                print(f"  LTT as UTC (no adjust): {dt_as_utc.strftime('%Y-%m-%d %H:%M:%S')} UTC")
                print(f"  LTT adjusted (-5.5h):   {dt_adjusted.strftime('%Y-%m-%d %H:%M:%S')} UTC")
                print(f"")

                # Check which one matches reality
                diff_no_adjust = abs((now_utc - dt_as_utc).total_seconds())
                diff_adjusted = abs((now_utc - dt_adjusted).total_seconds())

                print(f"  Diff (no adjust): {diff_no_adjust:.0f} seconds from now")
                print(f"  Diff (adjusted):  {diff_adjusted:.0f} seconds from now")

                if diff_no_adjust < diff_adjusted:
                    print(f"  >>> RESULT: Timestamp is ALREADY in UTC (no conversion needed)")
                else:
                    print(f"  >>> RESULT: Timestamp is in IST (needs -19800 adjustment)")

        print(f"\n{'='*60}")
        print("Test complete!")

asyncio.run(test_timestamp())

