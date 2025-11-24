"""
Example usage of DataSaver with ExchangeData for periodic market data collection.

This script demonstrates how to:
1. Create ExchangeData instances with start/end times
2. Add exchanges to DataSaver
3. Run periodic 5-minute saves to price_history_intraday
4. Aggregate to price_history_daily at end of day
"""

import asyncio
import time
from datetime import datetime, timedelta
from services.data.data_saver import DataSaver
from services.data.exchange_data import ExchangeData


async def main():
    # Initialize DataSaver
    data_saver = DataSaver()

    # Example 1: NSE (National Stock Exchange of India)
    # Market hours: 9:15 AM to 3:30 PM IST
    current_date = datetime.now()

    # Set start time to 9:15 AM today
    start_time = current_date.replace(hour=9, minute=15, second=0, microsecond=0)
    start_time_ms = int(start_time.timestamp() * 1000)

    # Set end time to 3:30 PM today
    end_time = current_date.replace(hour=15, minute=30, second=0, microsecond=0)
    end_time_ms = int(end_time.timestamp() * 1000)

    # For testing: Use near-future times (5 minutes from now to 15 minutes from now)
    # Uncomment below for actual testing
    test_start = int((time.time() + 60) * 1000)  # Start in 1 minute
    test_end = int((time.time() + 600) * 1000)   # End in 10 minutes

    nse_exchange = ExchangeData(
        exchange_name="NSE",
        exchange_id=1,  # Replace with actual NSE exchange_id from your database
        start_time=test_start,  # Use test_start for testing, start_time_ms for production
        end_time=test_end,      # Use test_end for testing, end_time_ms for production
        interval_minutes=5,      # Save every 5 minutes
    )

    # Example 2: BSE (Bombay Stock Exchange)
    # If you have multiple exchanges with different hours
    bse_exchange = ExchangeData(
        exchange_name="BSE",
        exchange_id=2,  # Replace with actual BSE exchange_id from your database
        start_time=test_start,
        end_time=test_end,
        interval_minutes=5,
    )

    # Add exchanges to DataSaver
    data_saver.add_exchange(nse_exchange)
    # data_saver.add_exchange(bse_exchange)  # Uncomment to add BSE

    # Print exchange info
    print("=" * 80)
    print("Exchange Configuration:")
    print("=" * 80)
    print(nse_exchange.get_exchange_info())
    print("=" * 80)

    # Start periodic saves for all exchanges
    await data_saver.start_all_exchanges()

    print(f"\n✓ Started periodic data collection for {len(data_saver.exchanges)} exchange(s)")
    print(f"  - Saving every {nse_exchange.interval_minutes} minutes to price_history_intraday")
    print(f"  - Will aggregate to price_history_daily at end of day")
    print(f"\nWaiting for completion...")

    # Wait for all exchanges to complete their data collection
    await data_saver.wait_for_completion()

    print("\n✓ All exchanges completed data collection")
    print("  - Intraday data saved to price_history_intraday table")
    print("  - Daily aggregated data saved to price_history_daily table")


async def manual_save_example():
    """
    Example of manually triggering saves without periodic scheduling.
    Useful for testing or one-off saves.
    """
    data_saver = DataSaver()

    # Save current data for all exchanges
    print("Saving current 5-minute data to intraday table...")
    count = await data_saver.save_to_intraday_table()
    print(f"✓ Saved {count} records")

    # Save for specific exchange only
    print("\nSaving data for exchange_id=1 only...")
    count = await data_saver.save_to_intraday_table(exchange_id=1)
    print(f"✓ Saved {count} records for exchange_id=1")

    # Manually aggregate to daily (e.g., at end of day job)
    exchange_data = ExchangeData(
        exchange_name="NSE",
        exchange_id=1,
        start_time=0,
        end_time=0,
    )

    # Get today's date timestamp (midnight)
    today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    date_timestamp = int(today.timestamp() * 1000)

    print(f"\nAggregating daily data for {today.strftime('%Y-%m-%d')}...")
    count = await data_saver.aggregate_and_save_daily(exchange_data, date_timestamp)
    print(f"✓ Saved {count} daily records")


if __name__ == "__main__":
    print("DataSaver Example Usage")
    print("=" * 80)
    print("\nChoose example to run:")
    print("1. Periodic Save (runs continuously until end_time)")
    print("2. Manual Save (one-time save)")

    choice = input("\nEnter choice (1 or 2): ").strip()

    if choice == "1":
        print("\nStarting periodic save example...")
        asyncio.run(main())
    elif choice == "2":
        print("\nRunning manual save example...")
        asyncio.run(manual_save_example())
    else:
        print("Invalid choice. Please run again and enter 1 or 2.")

