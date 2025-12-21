import asyncio
import sys
import os
from sqlalchemy import select, func, text

# Add project root to path
sys.path.append(os.getcwd())

from config.database_config import get_db_session
from models import PriceHistoryIntraday, Instrument

async def check_duplicates():
    async for session in get_db_session():
        print("Checking for duplicates in PriceHistoryIntraday...")

        # Check for count of records per instrument/datetime
        stmt = text("""
            SELECT instrument_id, datetime, count(*)
            FROM price_history_intraday
            GROUP BY instrument_id, datetime
            HAVING count(*) > 1
            LIMIT 10
        """)

        result = await session.execute(stmt)
        duplicates = result.fetchall()

        if duplicates:
            print(f"Found {len(duplicates)} duplicate groups (showing first 10):")
            for row in duplicates:
                print(f"InstID: {row[0]}, Time: {row[1]}, Count: {row[2]}")
        else:
            print("No exact duplicates found on (instrument_id, datetime).")

        # Check for records that are very close in time (potential mismatch)
        # This is harder to query efficiently in SQL without window functions or self join
        # Let's just check a specific instrument

        # Get an instrument ID
        inst_stmt = select(Instrument).limit(1)
        inst_result = await session.execute(inst_stmt)
        instrument = inst_result.scalar_one_or_none()

        if instrument:
            print(f"\nChecking records for {instrument.symbol} (ID: {instrument.id})...")
            stmt = select(PriceHistoryIntraday).where(
                PriceHistoryIntraday.instrument_id == instrument.id
            ).order_by(PriceHistoryIntraday.datetime).limit(20)

            result = await session.execute(stmt)
            records = result.scalars().all()

            print(f"Found {len(records)} records:")
            for r in records:
                print(f"ID: {r.id}, Time: {r.datetime}, Open: {r.open}, ResolveReq: {r.resolve_required}")

if __name__ == "__main__":
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(check_duplicates())

