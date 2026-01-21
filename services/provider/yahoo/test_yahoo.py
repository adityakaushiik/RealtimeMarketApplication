
import asyncio
import sys
import os
from datetime import datetime, timedelta, timezone

# Ensure project root is in python path
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, "..", "..", ".."))
sys.path.append(project_root)

from services.provider.yahoo_provider import YahooFinanceProvider
from models import Instrument

# Setup Logging
import logging
logging.basicConfig(level=logging.INFO)
logging.getLogger("config.logger").setLevel(logging.WARNING)

async def test_yahoo():
    print("--- Testing Yahoo Provider ---")
    
    provider = YahooFinanceProvider()
    
    # RELIANCE.NS on Yahoo
    test_instrument = Instrument(
        id=1, 
        symbol="RELIANCE.NS", 
        name="Reliance Industries",
        exchange_id=1, 
        instrument_type_id=1
    )
    
    print(f"Target Instrument: {test_instrument.symbol}")

    # --- Intraday ---
    print("\n[Test 1: Intraday Data]")
    now_utc = datetime.now(timezone.utc)
    start_time = now_utc - timedelta(days=5) # Max 60d, but 5m usually last few days
    
    print(f"Requesting Window (UTC): {start_time} -> {now_utc}")
    
    try:
        intraday_data = await provider.get_intraday_prices(
            [test_instrument], 
            start_date=start_time, 
            end_date=now_utc
        )
        
        if intraday_data and test_instrument.symbol in intraday_data:
            candles = intraday_data[test_instrument.symbol]
            print(f"Received {len(candles)} intraday candles.")
            if candles:
                last = candles[-1]
                print(f"Last Candle: {last.datetime} (iso: {last.datetime.isoformat()})")
                print(f"Timezone Info: {last.datetime.tzinfo}")
        else:
            print("No intraday data received.")
    except Exception as e:
        print(f"Intraday Error: {e}")

    # --- Daily ---
    print("\n[Test 2: Daily Data]")
    try:
        daily_data = await provider.get_daily_prices(
            [test_instrument],
            start_date=now_utc - timedelta(days=30),
            end_date=now_utc
        )
        
        if daily_data and test_instrument.symbol in daily_data:
            candles = daily_data[test_instrument.symbol]
            print(f"Received {len(candles)} daily candles.")
            if candles:
                last_daily = candles[-1]
                print(f"Last Daily Candle: {last_daily.datetime} (iso: {last_daily.datetime.isoformat()})")
                print(f"Timezone Info: {last_daily.datetime.tzinfo}")
    except Exception as e:
        print(f"Daily Error: {e}")

    # --- Tick ---
    print("\n[Test 3: Tick Data Analysis]")
    print("Yahoo provider message_handler uses:")
    print("ts = int(time.time() * 1000)")
    print("Local System Time (UTC Epoch Milliseconds)")

if __name__ == "__main__":
    asyncio.run(test_yahoo())
