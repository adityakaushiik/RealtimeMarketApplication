"""
Test script to demonstrate the duplicate policy fix.

This shows how the DUPLICATE_POLICY LAST allows multiple values at the same timestamp,
keeping only the most recent one.
"""
import asyncio
import time
from services.redis_timeseries import RedisTimeSeries


async def test_duplicate_values():
    """Test that duplicate timestamps are allowed with LAST policy."""

    print("=" * 80)
    print("DUPLICATE TIMESTAMP TEST")
    print("=" * 80)
    print()

    # Initialize RedisTimeSeries
    rts = RedisTimeSeries(retention_minutes=15)

    # Test symbol
    test_key = "TEST_SYMBOL_DUPLICATE"

    # Get current timestamp
    now_ms = int(time.time() * 1000)

    print(f"Test Symbol: {test_key}")
    print(f"Timestamp: {now_ms}")
    print()

    # Add first value at timestamp
    print("Adding first value: price=100.0, volume=1000")
    await rts.add_to_timeseries(test_key, now_ms, price=100.0, volume=1000.0)

    # Add second value at SAME timestamp (should replace first)
    print("Adding second value at SAME timestamp: price=101.0, volume=2000")
    await rts.add_to_timeseries(test_key, now_ms, price=101.0, volume=2000.0)

    # Add third value at SAME timestamp (should replace second)
    print("Adding third value at SAME timestamp: price=102.0, volume=3000")
    await rts.add_to_timeseries(test_key, now_ms, price=102.0, volume=3000.0)

    print()

    # Retrieve the latest value
    print("Retrieving latest value...")
    latest = await rts.get_last_traded_price(test_key)

    if latest:
        print(f"✓ Retrieved value: price={latest.price}, volume={latest.volume}")
        print(f"  Expected: price=102.0, volume=3000.0 (LAST value)")

        if latest.price == 102.0 and latest.volume == 3000.0:
            print("✓ SUCCESS: Duplicate policy is working correctly!")
        else:
            print("✗ FAIL: Expected last value but got different result")
    else:
        print("✗ FAIL: No data retrieved")

    print()
    print("=" * 80)
    print()
    print("Explanation:")
    print("- With DUPLICATE_POLICY LAST, Redis keeps only the most recent value")
    print("- Multiple ticks at the same millisecond are allowed")
    print("- This is essential for real-time market data ingestion")
    print("=" * 80)


if __name__ == "__main__":
    asyncio.run(test_duplicate_values())

