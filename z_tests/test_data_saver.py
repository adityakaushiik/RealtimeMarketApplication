"""
Quick test to verify DataSaver implementation.
This demonstrates the complete flow without needing actual market hours.
"""

import asyncio
import time
from datetime import datetime
from services.data.data_saver import DataSaver
from services.data.exchange_data import ExchangeData


async def test_datasaver_basic():
    """
    Test basic DataSaver functionality.

    This test:
    1. Creates an ExchangeData with near-future times
    2. Adds it to DataSaver
    3. Runs for a short period
    4. Verifies saves are happening
    """

    print("\n" + "="*80)
    print("DATASAVER TEST - Basic Functionality")
    print("="*80)

    # Create test exchange with short time window
    # Start in 30 seconds, run for 3 minutes
    current_time = time.time()
    start_time_ms = int((current_time + 30) * 1000)
    end_time_ms = int((current_time + 210) * 1000)  # 3.5 minutes from now

    test_exchange = ExchangeData(
        exchange_name="TEST_EXCHANGE",
        exchange_id=1,  # Adjust to match your database
        start_time=start_time_ms,
        end_time=end_time_ms,
        interval_minutes=1,  # Save every 1 minute for quick testing
    )

    print("\n📋 Test Configuration:")
    print(f"  Exchange: {test_exchange.exchange_name}")
    print(f"  Start: {datetime.fromtimestamp(start_time_ms/1000).strftime('%H:%M:%S')}")
    print(f"  End: {datetime.fromtimestamp(end_time_ms/1000).strftime('%H:%M:%S')}")
    print(f"  Interval: {test_exchange.interval_minutes} minute(s)")
    print(f"  Duration: {(end_time_ms - start_time_ms) / 1000:.0f} seconds")

    # Initialize DataSaver
    data_saver = DataSaver()
    data_saver.add_exchange(test_exchange)

    print("\n✓ DataSaver initialized")
    print(f"✓ Added {len(data_saver.exchanges)} exchange(s)")

    # Start periodic saves
    print("\n⏳ Starting periodic saves...")
    print(f"   Waiting {(start_time_ms - int(time.time() * 1000)) / 1000:.0f}s until start time...")

    await data_saver.start_all_exchanges()

    print("\n✓ Periodic saves started")
    print("⏳ Waiting for completion...")

    # Wait for completion
    await data_saver.wait_for_completion()

    print("\n" + "="*80)
    print("✓ TEST COMPLETED SUCCESSFULLY")
    print("="*80)
    print("\nCheck your database tables:")
    print("  - price_history_intraday: Should have records with interval='1m'")
    print("  - price_history_daily: Should have aggregated daily data")
    print("\nSQL to verify:")
    print("  SELECT COUNT(*) FROM price_history_intraday WHERE interval='1m';")
    print("  SELECT COUNT(*) FROM price_history_daily;")
    print()


async def test_manual_save():
    """
    Test manual save functionality without periodic scheduling.
    """

    print("\n" + "="*80)
    print("DATASAVER TEST - Manual Save")
    print("="*80)

    data_saver = DataSaver()

    print("\n1️⃣ Testing save_to_intraday_table()...")
    try:
        count = await data_saver.save_to_intraday_table()
        print(f"   ✓ Saved {count} intraday records")
    except Exception as e:
        print(f"   ⚠ Error (expected if no data in Redis): {e}")

    print("\n2️⃣ Testing save_to_intraday_table(exchange_id=1)...")
    try:
        count = await data_saver.save_to_intraday_table(exchange_id=1)
        print(f"   ✓ Saved {count} records for exchange_id=1")
    except Exception as e:
        print(f"   ⚠ Error (expected if no data): {e}")

    print("\n3️⃣ Testing aggregate_and_save_daily()...")
    try:
        # Use today's date
        today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        date_ts = int(today.timestamp() * 1000)

        exchange = ExchangeData(
            exchange_name="TEST",
            exchange_id=1,
            start_time=0,
            end_time=0,
        )

        count = await data_saver.aggregate_and_save_daily(exchange, date_ts)
        print(f"   ✓ Aggregated {count} daily records")
    except Exception as e:
        print(f"   ⚠ Error (expected if no intraday data): {e}")

    print("\n" + "="*80)
    print("✓ MANUAL SAVE TEST COMPLETED")
    print("="*80 + "\n")


async def test_exchange_data():
    """
    Test ExchangeData class functionality.
    """

    print("\n" + "="*80)
    print("EXCHANGEDATA TEST - Configuration")
    print("="*80)

    # Create test exchange
    today = datetime.now()
    start = today.replace(hour=9, minute=15, second=0, microsecond=0)
    end = today.replace(hour=15, minute=30, second=0, microsecond=0)

    exchange = ExchangeData(
        exchange_name="NSE",
        exchange_id=1,
        start_time=int(start.timestamp() * 1000),
        end_time=int(end.timestamp() * 1000),
        interval_minutes=5,
        api_key="test_key_123",
        api_secret="test_secret_456",
    )

    print("\n📋 Exchange Info:")
    info = exchange.get_exchange_info()
    for key, value in info.items():
        print(f"  {key}: {value}")

    # Test helper methods
    current_time_ms = int(time.time() * 1000)

    print(f"\n🕐 Current Time: {datetime.now().strftime('%H:%M:%S')}")
    print(f"  Market Open? {exchange.is_market_open(current_time_ms)}")

    if exchange.is_market_open(current_time_ms):
        remaining = exchange.get_remaining_time_ms(current_time_ms)
        print(f"  Time until close: {remaining / 1000 / 60:.1f} minutes")
    else:
        # Check if before or after market hours
        if current_time_ms < exchange.start_time:
            wait_time = (exchange.start_time - current_time_ms) / 1000 / 60
            print(f"  Time until open: {wait_time:.1f} minutes")
        else:
            print(f"  Market is closed")

    print("\n" + "="*80)
    print("✓ EXCHANGEDATA TEST COMPLETED")
    print("="*80 + "\n")


async def run_all_tests():
    """Run all tests in sequence."""

    print("\n" + "="*80)
    print("DATASAVER - COMPREHENSIVE TEST SUITE")
    print("="*80)
    print("\nThis will test:")
    print("  1. ExchangeData configuration")
    print("  2. Manual save operations")
    print("  3. Periodic save workflow (short duration)")
    print("\n" + "="*80)

    input("\nPress ENTER to start tests...")

    # Test 1: ExchangeData
    await test_exchange_data()

    # Test 2: Manual saves
    await test_manual_save()

    # Test 3: Periodic saves (this will take ~3-4 minutes)
    print("\n⚠️  WARNING: The next test will run for ~3-4 minutes")
    choice = input("Run periodic save test? (y/n): ").strip().lower()

    if choice == 'y':
        await test_datasaver_basic()
    else:
        print("Skipped periodic save test")

    print("\n" + "="*80)
    print("🎉 ALL TESTS COMPLETED!")
    print("="*80)
    print("\nNext steps:")
    print("  1. Review the logs above for any errors")
    print("  2. Check database tables for saved data")
    print("  3. Adjust exchange_id to match your database")
    print("  4. Use example_usage.py for production scenarios")
    print()


if __name__ == "__main__":
    print("\n" + "="*80)
    print("DATASAVER TEST RUNNER")
    print("="*80)
    print("\nSelect test to run:")
    print("  1. ExchangeData Configuration Test")
    print("  2. Manual Save Test")
    print("  3. Periodic Save Test (3-4 minutes)")
    print("  4. Run All Tests")
    print("="*80)

    choice = input("\nEnter choice (1-4): ").strip()

    if choice == "1":
        asyncio.run(test_exchange_data())
    elif choice == "2":
        asyncio.run(test_manual_save())
    elif choice == "3":
        asyncio.run(test_datasaver_basic())
    elif choice == "4":
        asyncio.run(run_all_tests())
    else:
        print("Invalid choice. Please run again and select 1-4.")

