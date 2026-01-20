"""
Integration test for Dhan API intraday price fetching.
Tests that the securityId and exchangeSegment are correctly formatted.
"""
import asyncio
import sys
import os

# Fix Unicode output on Windows
sys.stdout.reconfigure(encoding='utf-8', errors='replace')

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from datetime import datetime, timezone, timedelta
from config.logger import logger


async def test_get_intraday_prices():
    """Test the get_intraday_prices method to verify payload construction."""
    from services.provider.provider_manager import ProviderManager
    from models import Instrument

    print("\n=== Testing get_intraday_prices Payload ===\n")

    # Initialize provider manager
    pm = ProviderManager()
    await pm.initialize()

    # Get dhan provider
    dhan = pm.providers.get("DHAN")
    if not dhan:
        print("❌ DHAN provider not found")
        return False

    # Create mock instruments for testing
    # These should match actual instruments in your DB
    test_instruments = [
        Instrument(id=1, symbol="NIFTY", exchange_id=2, instrument_type_id=2),
        Instrument(id=2, symbol="BANKNIFTY", exchange_id=2, instrument_type_id=2),
        Instrument(id=3, symbol="HDFCBANK", exchange_id=2, instrument_type_id=1),
        Instrument(id=4, symbol="SENSEX", exchange_id=3, instrument_type_id=2),
    ]

    # Set date range
    end_date = datetime.now(timezone.utc)
    start_date = end_date - timedelta(hours=2)

    print(f"Date range: {start_date} to {end_date}")
    print(f"Testing {len(test_instruments)} instruments...\n")

    # Patch _make_request to capture the payload without actually making the request
    original_make_request = dhan._make_request
    captured_payloads = []

    async def mock_make_request(endpoint: str, payload: dict) -> dict:
        captured_payloads.append(payload)
        # Don't actually make the request, just capture and return empty
        return {}

    dhan._make_request = mock_make_request

    try:
        # Call get_intraday_prices
        await dhan.get_intraday_prices(
            instruments=test_instruments,
            start_date=start_date,
            end_date=end_date
        )
    finally:
        # Restore original method
        dhan._make_request = original_make_request

    # Analyze captured payloads
    print("Captured API payloads:")
    print("-" * 60)

    all_valid = True
    for i, payload in enumerate(captured_payloads):
        sec_id = payload.get("securityId", "")
        ex_seg = payload.get("exchangeSegment", "")

        # Check for issues
        issues = []
        if ":" in str(sec_id):
            issues.append(f"securityId contains ':' - should be numeric only")
        if not str(sec_id).isdigit():
            issues.append(f"securityId is not numeric: {sec_id}")
        if not ex_seg:
            issues.append("exchangeSegment is empty")

        status = "✅" if not issues else "❌"
        print(f"\n{status} Payload {i+1}:")
        print(f"   securityId: {sec_id}")
        print(f"   exchangeSegment: {ex_seg}")
        print(f"   fromDate: {payload.get('fromDate')}")
        print(f"   toDate: {payload.get('toDate')}")

        if issues:
            all_valid = False
            for issue in issues:
                print(f"   ⚠️  {issue}")

    # Cleanup - disconnect providers
    for provider in pm.providers.values():
        if hasattr(provider, 'disconnect_websocket'):
            provider.disconnect_websocket()

    return all_valid


async def test_resolve_workflow():
    """Test the full resolution workflow for intraday data."""
    from services.data.redis_mapping import get_redis_mapping_helper

    print("\n=== Testing Resolution Workflow ===\n")

    mapper = get_redis_mapping_helper()

    # Test cases: symbol -> (expected_segment, expected_security_id)
    test_cases = [
        ("NIFTY", "IDX_I", "13"),
        ("BANKNIFTY", "IDX_I", "25"),
        ("SENSEX", "IDX_I", "51"),
        ("HDFCBANK", "NSE_EQ", "1333"),
    ]

    all_passed = True

    for symbol, expected_seg, expected_id in test_cases:
        # Get from Redis
        segment = await mapper.get_provider_segment("DHAN", symbol)
        provider_symbol = await mapper.get_provider_symbol("DHAN", symbol)

        # Parse provider_symbol if it's composite
        if provider_symbol and ":" in str(provider_symbol):
            _, actual_id = str(provider_symbol).split(":", 1)
        else:
            actual_id = str(provider_symbol) if provider_symbol else None

        # Check results
        seg_ok = segment == expected_seg
        id_ok = actual_id == expected_id

        status = "✅" if (seg_ok and id_ok) else "❌"
        print(f"{status} {symbol}:")
        print(f"   Segment: {segment} (expected: {expected_seg}) {'✓' if seg_ok else '✗'}")
        print(f"   SecurityID: {actual_id} (expected: {expected_id}) {'✓' if id_ok else '✗'}")

        if not (seg_ok and id_ok):
            all_passed = False

    return all_passed


async def main():
    print("=" * 60)
    print("Dhan API Integration Tests")
    print("=" * 60)

    results = {}

    # Test 1: Resolution workflow
    try:
        results["resolution"] = await test_resolve_workflow()
    except Exception as e:
        print(f"❌ Resolution test failed: {e}")
        import traceback
        traceback.print_exc()
        results["resolution"] = False

    # Test 2: Get intraday prices payload
    try:
        results["intraday_payload"] = await test_get_intraday_prices()
    except Exception as e:
        print(f"❌ Intraday payload test failed: {e}")
        import traceback
        traceback.print_exc()
        results["intraday_payload"] = False

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

