"""
Test Dhan API timestamp handling to verify if timestamps are UTC or IST.
"""
import asyncio
import sys
import os
from datetime import datetime, timezone, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytz
from sqlalchemy import select
from config.database_config import get_db_session
from models.instruments import Instrument


async def main():
    print("=" * 60)
    print("Dhan Timestamp Investigation")
    print("=" * 60)

    # First, let's check what's in the database for NIFTY
    print("\n1. Checking DB records for NIFTY:")

    async for session in get_db_session():
        # Get NIFTY instrument
        result = await session.execute(
            select(Instrument).where(Instrument.symbol == "NIFTY")
        )
        nifty = result.scalar_one_or_none()

        if not nifty:
            print("   NIFTY not found in DB!")
            return

        print(f"   NIFTY instrument ID: {nifty.id}")

        # Get last 10 intraday records for NIFTY
        from models.price_history_intraday import PriceHistoryIntraday

        result = await session.execute(
            select(PriceHistoryIntraday)
            .where(PriceHistoryIntraday.instrument_id == nifty.id)
            .order_by(PriceHistoryIntraday.datetime.desc())
            .limit(20)
        )
        records = result.scalars().all()

        print(f"\n2. Last 20 DB records for NIFTY (instrument_id={nifty.id}):")
        print(f"   {'Datetime (stored)':<30} {'UTC':<25} {'IST (if stored as UTC)':<25}")
        print("-" * 80)

        ist_tz = pytz.timezone("Asia/Kolkata")

        for r in records:
            stored_dt = r.datetime
            # If stored datetime is naive, assume UTC
            if stored_dt.tzinfo is None:
                stored_dt = stored_dt.replace(tzinfo=timezone.utc)

            # Convert to UTC and IST for display
            utc_dt = stored_dt.astimezone(timezone.utc)
            ist_dt = stored_dt.astimezone(ist_tz)

            print(f"   {str(r.datetime):<30} {str(utc_dt):<25} {str(ist_dt):<25}")

        print("\n3. Analysis:")
        if records:
            # NSE market opens at 9:15 IST = 3:45 UTC
            # NSE market closes at 15:30 IST = 10:00 UTC

            latest = records[0].datetime
            if latest.tzinfo is None:
                latest = latest.replace(tzinfo=timezone.utc)

            latest_utc = latest.astimezone(timezone.utc)
            latest_ist = latest.astimezone(ist_tz)

            print(f"   Latest record:")
            print(f"     - As stored: {records[0].datetime}")
            print(f"     - As UTC: {latest_utc.strftime('%Y-%m-%d %H:%M:%S %Z')}")
            print(f"     - As IST: {latest_ist.strftime('%Y-%m-%d %H:%M:%S %Z')}")

            # Check if the UTC time makes sense for NSE trading hours
            utc_hour = latest_utc.hour
            print(f"\n   If timestamps are stored as UTC:")
            print(f"     - UTC hour {utc_hour}:XX corresponds to IST hour {(utc_hour + 5) % 24}:{(utc_hour + 5) // 24 * 30}:XX")
            print(f"     - NSE trades from 3:45 UTC to 10:00 UTC (9:15 IST to 15:30 IST)")

            if 3 <= utc_hour <= 10:
                print(f"     ✅ Hour {utc_hour} UTC is within NSE trading hours - timestamps are likely UTC")
            elif 9 <= utc_hour <= 16:
                print(f"     ⚠️ Hour {utc_hour} UTC looks like IST trading hours - timestamps might be IST stored as UTC!")

        break


if __name__ == "__main__":
    asyncio.run(main())

