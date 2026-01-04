"""
Test script to explore dhanhq library structure
"""
import sys
sys.path.insert(0, r"C:\Users\tech\OneDrive\Documents\GitHub\RealtimeMarketApplication")

# Test imports
try:
    import dhanhq
    print("✅ dhanhq imported successfully")

    from dhanhq import marketfeed
    print("✅ marketfeed imported successfully")
    print(f"   File: {marketfeed.__file__}")

    # Check available constants
    print("\n📊 Exchange Segment Constants:")
    print(f"   NSE = {marketfeed.NSE}")
    print(f"   BSE = {marketfeed.BSE}")
    print(f"   NSE_FNO = {marketfeed.NSE_FNO}")
    print(f"   MCX = {marketfeed.MCX}")

    print("\n📊 Subscription Mode Constants:")
    print(f"   Ticker = {marketfeed.Ticker}")
    print(f"   Quote = {marketfeed.Quote}")
    print(f"   Full = {marketfeed.Full}")
    print(f"   Depth = {marketfeed.Depth}")

    # Check DhanFeed class
    print("\n📊 DhanFeed class methods:")
    methods = [m for m in dir(marketfeed.DhanFeed) if not m.startswith('_')]
    for m in methods:
        print(f"   - {m}")

    # Check dhanhq main class
    from dhanhq import dhanhq as DhanHQ
    print("\n📊 DhanHQ class methods (for REST API):")
    methods = [m for m in dir(DhanHQ) if not m.startswith('_')]
    for m in methods[:20]:
        print(f"   - {m}")
    if len(methods) > 20:
        print(f"   ... and {len(methods) - 20} more")

except Exception as e:
    print(f"❌ Error: {e}")
    import traceback
    traceback.print_exc()

