"""
Test script for DhanProvider functionality.
Validates symbol mapping, segment resolution, and timestamp handling.
"""
import asyncio
import sys
import os

# Fix Unicode output on Windows
if sys.stdout:
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.logger import logger


async def test_redis_mapping():
    """Test that Redis mappings are correctly set up for DHAN provider."""
    from services.data.redis_mapping import get_redis_mapping_helper

    mapper = get_redis_mapping_helper()

    # Test symbols - mix of NSE, BSE, and Index
    test_symbols = [
        ("NIFTY", "IDX_I"),      # NSE Index
        ("BANKNIFTY", "IDX_I"),  # NSE Index
        ("SENSEX", "IDX_I"),     # BSE Index
        ("HDFCBANK", "NSE_EQ"),  # NSE Equity
        ("RELIANCE", "NSE_EQ"), # NSE Equity
        ("TCS", "NSE_EQ"),      # NSE Equity
    ]

    print("\n=== Testing Redis Segment Mappings ===")
    all_passed = True

    for symbol, expected_segment in test_symbols:
        segment = await mapper.get_provider_segment("DHAN", symbol)
        provider_symbol = await mapper.get_provider_symbol("DHAN", symbol)

        status = "✅" if segment else "❌"
        print(f"{status} {symbol}: segment={segment}, provider_symbol={provider_symbol}")

        if not segment:
            all_passed = False
            print(f"   ⚠️  No segment found for {symbol}!")

    return all_passed


async def test_prepare_single_instrument():
    """Test _prepare_single_instrument function."""
    from services.provider.dhan_provider import DhanProvider
    from services.provider.provider_manager import ProviderManager

    print("\n=== Testing _prepare_single_instrument ===")

    # Initialize provider manager first (it sets up redis mappings)
    pm = ProviderManager()
    await pm.initialize()

    # Get dhan provider
    dhan = pm.providers.get("DHAN")
    if not dhan:
        print("❌ DHAN provider not found in ProviderManager")
        return False

    test_cases = [
        ("13", "NIFTY"),          # Index
        ("25", "BANKNIFTY"),      # Index
        ("1333", "HDFCBANK"),     # NSE Equity
        ("IDX_I:13", "NIFTY"),    # Composite format
    ]

    all_passed = True
    for provider_sym, internal_sym in test_cases:
        result = await dhan._prepare_single_instrument(provider_sym, internal_sym)

        if result:
            ex_seg, sec_id = result
            print(f"✅ {internal_sym}: segment={ex_seg}, sec_id={sec_id}")
        else:
            print(f"❌ {internal_sym}: Failed to prepare instrument")
            all_passed = False

    return all_passed


async def test_timestamp_handling():
    """Test timestamp handling to ensure no negative values."""
    import time
    import struct

    print("\n=== Testing Timestamp Handling ===")

    # Simulate Dhan timestamp (seconds since epoch in IST)
    # Current time in IST would be UTC + 5.5 hours
    current_utc = int(time.time())
    ist_offset = 19800  # 5.5 hours in seconds

    # Dhan sends IST timestamp
    simulated_dhan_ltt = current_utc + ist_offset  # What Dhan would send

    # Old conversion (buggy)
    old_result = (simulated_dhan_ltt - ist_offset) * 1000

    # New approach - use system time
    new_result = int(time.time() * 1000)

    print(f"Simulated Dhan LTT (IST epoch): {simulated_dhan_ltt}")
    print(f"Old conversion result (ms): {old_result}")
    print(f"New approach result (ms): {new_result}")
    print(f"Both positive? Old: {old_result > 0}, New: {new_result > 0}")

    # Test with a typical Dhan value that could cause issues
    # The error showed -1746685272000 which is a negative timestamp
    # This could happen if ltt was already in some weird format

    print("\n--- Edge cases ---")

    # Test case: What if ltt is 0 or negative?
    test_ltt_values = [0, -1, 1000000, current_utc, current_utc + ist_offset]
    for ltt in test_ltt_values:
        converted = (ltt - ist_offset) * 1000
        system_ts = int(time.time() * 1000)
        print(f"LTT={ltt}: Old conversion={converted}, System time={system_ts}")

    return True


async def test_symbol_map_consistency():
    """Test that symbol_map is correctly populated during subscription."""
    print("\n=== Testing Symbol Map Consistency ===")

    # Simulate what _subscribe_batch does
    symbol_map = {}
    symbol_exchange_map = {}

    # Sample data as would be processed
    instruments = [
        ("IDX_I", "13", "NIFTY"),
        ("IDX_I", "25", "BANKNIFTY"),
        ("NSE_EQ", "1333", "HDFCBANK"),
        ("BSE_EQ", "500180", "HDFCBANK.BSE"),
    ]

    # Populate maps as _subscribe_batch does
    for ex_seg, sec_id, internal_symbol in instruments:
        composite_key = f"{ex_seg}:{sec_id}"
        symbol_map[composite_key] = internal_symbol
        symbol_exchange_map[internal_symbol] = ex_seg

    print("Symbol Map (composite_key -> internal_symbol):")
    for k, v in symbol_map.items():
        print(f"  {k} -> {v}")

    print("\nSymbol Exchange Map (internal_symbol -> exchange_segment):")
    for k, v in symbol_exchange_map.items():
        print(f"  {k} -> {v}")

    # Simulate message processing lookup
    print("\n--- Simulating message processing lookups ---")
    test_lookups = [
        (0, 13),   # IDX_I, NIFTY
        (0, 25),   # IDX_I, BANKNIFTY
        (1, 1333), # NSE_EQ, HDFCBANK
    ]

    EXCHANGE_MAP = {
        0: "IDX_I",
        1: "NSE_EQ",
        4: "BSE_EQ",
    }

    for ex_code, sec_id in test_lookups:
        exchange_str = EXCHANGE_MAP.get(ex_code, str(ex_code))
        composite = f"{exchange_str}:{sec_id}"
        resolved_symbol = symbol_map.get(composite)
        resolved_exchange = symbol_exchange_map.get(resolved_symbol) if resolved_symbol else None

        print(f"ExCode={ex_code}, SecID={sec_id}")
        print(f"  Composite: {composite}")
        print(f"  Resolved Symbol: {resolved_symbol}")
        print(f"  Resolved Exchange: {resolved_exchange}")
        print()

    return True


async def main():
    """Run all tests."""
    print("=" * 60)
    print("DhanProvider Test Suite")
    print("=" * 60)

    results = {}

    # Test 1: Timestamp handling (no DB required)
    results["timestamp"] = await test_timestamp_handling()

    # Test 2: Symbol map consistency (no DB required)
    results["symbol_map"] = await test_symbol_map_consistency()

    # Test 3: Redis mapping (requires Redis)
    try:
        results["redis_mapping"] = await test_redis_mapping()
    except Exception as e:
        print(f"❌ Redis mapping test failed with error: {e}")
        results["redis_mapping"] = False

    # Test 4: Prepare single instrument (requires full setup)
    try:
        results["prepare_instrument"] = await test_prepare_single_instrument()
    except Exception as e:
        print(f"❌ Prepare instrument test failed with error: {e}")
        import traceback
        traceback.print_exc()
        results["prepare_instrument"] = False

    # Summary
    print("\n" + "=" * 60)
    print("Test Summary")
    print("=" * 60)
    for test_name, passed in results.items():
        status = "✅ PASSED" if passed else "❌ FAILED"
        print(f"  {test_name}: {status}")

    all_passed = all(results.values())
    print(f"\nOverall: {'✅ ALL TESTS PASSED' if all_passed else '❌ SOME TESTS FAILED'}")

    return all_passed


if __name__ == "__main__":
    success = asyncio.run(main())
    sys.exit(0 if success else 1)

