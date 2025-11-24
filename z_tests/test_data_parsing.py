"""
Test script to verify data parsing logic works correctly
"""

# Sample data exactly as returned by Redis
data = {
    'AAPL': {'ts': 1763959200000, 'open': 271.55, 'high': 271.64, 'low': 271.55, 'close': 271.63, 'volume': 0.0},
    'ADANIPORTS.NS': {'ts': 1763959200000, 'open': 1484.0, 'high': 1484.6, 'low': 1482.8, 'close': 1483.0, 'volume': 11639467.0},
    'ASML': None,
    'AZN': None,
    'COST': None,
    'ULTRACEMCO.NS': {'ts': 1763959200000, 'open': 11638.0, 'high': 11645.0, 'low': 11635.0, 'close': 11641.0, 'volume': 309558.0},
}

print("Original data:")
print(f"Total symbols: {len(data)}")
print(f"None values: {sum(1 for v in data.values() if v is None)}")
print()

# Filter out None values (exactly as in data_saver.py)
valid_data = {k: v for k, v in data.items() if v is not None}

print("After filtering None values:")
print(f"Valid symbols: {len(valid_data)}")
print(f"Filtered out: {len(data) - len(valid_data)}")
print()

if not valid_data:
    print("❌ ERROR: No valid OHLCV data found")
else:
    print("✅ SUCCESS: Valid data found!")
    print()
    print("Valid symbols:")
    for symbol, ohlcv in valid_data.items():
        print(f"  {symbol}: O={ohlcv['open']}, H={ohlcv['high']}, L={ohlcv['low']}, C={ohlcv['close']}, V={ohlcv['volume']}")

    print()
    print("Checking OHLC completeness:")
    complete_count = 0
    incomplete_count = 0

    for symbol, ohlcv in valid_data.items():
        if all(ohlcv.get(k) is not None for k in ["open", "high", "low", "close"]):
            complete_count += 1
            print(f"  ✅ {symbol}: Complete OHLC data")
        else:
            incomplete_count += 1
            print(f"  ❌ {symbol}: Incomplete OHLC data")
            print(f"     Missing: {[k for k in ['open', 'high', 'low', 'close'] if ohlcv.get(k) is None]}")

    print()
    print(f"Summary: {complete_count} complete, {incomplete_count} incomplete")

