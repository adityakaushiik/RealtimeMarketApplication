"""
Test to compare Quote mode (17) vs Full mode (21) tick rates from Dhan WebSocket.
"""
import asyncio
import struct
import json
from datetime import datetime
import pytz

import websockets
import sys
sys.path.insert(0, r"C:\Users\tech\OneDrive\Documents\GitHub\RealtimeMarketApplication")

from config.settings import get_settings

settings = get_settings()
ist_tz = pytz.timezone('Asia/Kolkata')

async def test_mode(mode_name: str, request_code: int, duration: int = 30):
    """Test a specific subscription mode."""
    url = f"wss://api-feed.dhan.co?version=2&token={settings.DHAN_ACCESS_TOKEN}&clientId={settings.DHAN_CLIENT_ID}&authType=2"

    print(f"\n{'='*60}")
    print(f"Testing {mode_name} (Request Code: {request_code})")
    print(f"{'='*60}")

    tick_count = 0
    symbols_data = {}

    async with websockets.connect(url, ping_interval=30, ping_timeout=10) as ws:
        print("Connected! Subscribing to RELIANCE (2885), KOTAKBANK (1922), INFY (1594)...")

        payload = {
            "RequestCode": request_code,
            "InstrumentCount": 3,
            "InstrumentList": [
                {"ExchangeSegment": "NSE_EQ", "SecurityId": "2885"},
                {"ExchangeSegment": "NSE_EQ", "SecurityId": "1922"},
                {"ExchangeSegment": "NSE_EQ", "SecurityId": "1594"}
            ]
        }
        await ws.send(json.dumps(payload))
        print(f"Subscribed. Collecting data for {duration} seconds...")

        start_time = asyncio.get_event_loop().time()

        while (asyncio.get_event_loop().time() - start_time) < duration:
            try:
                message = await asyncio.wait_for(ws.recv(), timeout=5)

                if len(message) < 8:
                    continue

                response_code = struct.unpack('<B', message[0:1])[0]
                security_id = struct.unpack('<I', message[4:8])[0]

                if response_code in [2, 4, 8]:  # Ticker, Quote, Full
                    tick_count += 1

                    if security_id not in symbols_data:
                        symbols_data[security_id] = {
                            'ticks': 0,
                            'prices': set(),
                            'response_codes': set()
                        }

                    symbols_data[security_id]['ticks'] += 1
                    symbols_data[security_id]['response_codes'].add(response_code)

                    # Extract price based on response code
                    if len(message) >= 12:
                        ltp = struct.unpack('<f', message[8:12])[0]
                        symbols_data[security_id]['prices'].add(round(ltp, 2))

            except asyncio.TimeoutError:
                continue

    elapsed = duration
    print(f"\nResults for {mode_name}:")
    print(f"  Total ticks: {tick_count}")
    print(f"  Ticks/second: {tick_count/elapsed:.2f}")
    print(f"\nBy Symbol:")
    for sid, data in symbols_data.items():
        print(f"  {sid}: {data['ticks']} ticks, {len(data['prices'])} unique prices, response codes: {data['response_codes']}")

    return tick_count, symbols_data


async def main():
    print("="*60)
    print("COMPARING QUOTE MODE (17) vs FULL MODE (21)")
    print("="*60)
    print(f"Current Time: {datetime.now(ist_tz).strftime('%Y-%m-%d %H:%M:%S')} IST")

    duration = 30

    # Test Quote mode
    quote_ticks, quote_data = await test_mode("QUOTE MODE", 17, duration)

    # Small break between tests
    print("\nWaiting 5 seconds before next test...")
    await asyncio.sleep(5)

    # Test Full mode
    full_ticks, full_data = await test_mode("FULL MODE", 21, duration)

    # Summary
    print(f"\n{'='*60}")
    print("SUMMARY")
    print(f"{'='*60}")
    print(f"Quote Mode: {quote_ticks} ticks ({quote_ticks/duration:.2f}/s)")
    print(f"Full Mode:  {full_ticks} ticks ({full_ticks/duration:.2f}/s)")

    if full_ticks > quote_ticks:
        print(f"\nFull mode is {((full_ticks/quote_ticks)-1)*100:.1f}% better")
    elif quote_ticks > full_ticks:
        print(f"\nQuote mode is {((quote_ticks/full_ticks)-1)*100:.1f}% better")
    else:
        print("\nBoth modes are equal")


if __name__ == "__main__":
    asyncio.run(main())

