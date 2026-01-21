
import asyncio
import sys
import os
import pytz
from datetime import datetime, timedelta, timezone

# Ensure project root is in python path
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, "..", "..", ".."))
sys.path.append(project_root)

from services.provider.dhan_provider import DhanProvider
from models import Instrument, InstrumentType

# Setup Logging to avoid clutter
import logging
logging.basicConfig(level=logging.INFO)
# Mute some noisy loggers
logging.getLogger("config.logger").setLevel(logging.WARNING)

async def test_dhan():
    print("--- Testing Dhan Provider (Static Checks & Historical) ---")
    
    # Initialize Provider (without manager)
    provider = DhanProvider(provider_manager=None)
    
    # Create a test instrument (HDFCBANK Equity on NSE)
    # Using security ID 1333 directly as symbol to bypass mapping if needed
    test_instrument = Instrument(
        id=1,
        symbol="1333", 
        name="HDFCBANK",
        exchange_id=1, 
        instrument_type_id=1
    )
    
    print(f"Target Instrument: {test_instrument.symbol} (NSE HDFCBANK)")

    # --- Test 2: Intraday Data ---
    print("\n[Test 2: Intraday Data (5m)]")
    now_utc = datetime.now(timezone.utc)
    # Fetch last 2 days to ensure we get some market data
    start_time = now_utc - timedelta(days=5) 
    end_time = now_utc

    print(f"Requesting Window (UTC): {start_time} -> {end_time}")
    
    try:
        intraday_data = await provider.get_intraday_prices(
            [test_instrument], 
            start_date=start_time, 
            end_date=end_time
        )
        
        if intraday_data and test_instrument.symbol in intraday_data:
            candles = intraday_data[test_instrument.symbol]
            print(f"Received {len(candles)} intraday candles.")
            if candles:
                first = candles[0]
                last = candles[-1]
                print(f"First Candle: {first.datetime} (iso: {first.datetime.isoformat()})")
                print(f"Last Candle:  {last.datetime} (iso: {last.datetime.isoformat()})")
                print(f"Timezone Info: {first.datetime.tzinfo}")
        else:
            print("No intraday data received. (Check network/credentials/market hours)")

    except Exception as e:
        print(f"Intraday Error: {e}")

    # --- Test 3: Daily Data ---
    print("\n[Test 3: Daily Data]")
    start_daily = now_utc - timedelta(days=10)
    
    try:
        daily_data = await provider.get_daily_prices(
            [test_instrument],
            start_date=start_daily,
            end_date=now_utc
        )
        
        if daily_data and test_instrument.symbol in daily_data:
            candles = daily_data[test_instrument.symbol]
            print(f"Received {len(candles)} daily candles.")
            if candles:
                last_daily = candles[-1]
                print(f"Last Daily Candle: {last_daily.datetime} (iso: {last_daily.datetime.isoformat()})")
                print(f"Timezone Info: {last_daily.datetime.tzinfo}")
        else:
            print("No daily data received.")

    except Exception as e:
        print(f"Daily Error: {e}")

    # --- Test 1: Tick Data (Analysis) ---
    print("\n[Test 1: Tick Time Analysis by Code Inspection]")
    print("Function: message_handler / _process_message")
    print("For Quote (Code 4):")
    print("  Uses `ts = int(time.time() * 1000)`")
    print("  Local System Time (UTC Epoch Milliseconds)")
    print("For Ticker (Code 2):")
    print("  Extracts LTT (Last Traded Time) from packet.")
    print("  LTT is Epoch Seconds (int32).")
    
    await provider.close_session()

if __name__ == "__main__":
    asyncio.run(test_dhan())
