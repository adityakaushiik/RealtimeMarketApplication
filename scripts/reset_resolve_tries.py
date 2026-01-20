"""
Script to reset resolve_tries for records that need resolution.
"""
import asyncio
import sys
import os

if sys.stdout:
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from datetime import datetime, timezone, timedelta
from sqlalchemy import update
from config.database_config import get_db_session
from models import PriceHistoryIntraday


async def reset_tries():
    """Reset resolve_tries for records that need resolution"""
    async for session in get_db_session():
        # Reset records that have VALID data (close > 0) but still have resolve_required=True
        # These were successfully filled but the flag wasn't cleared
        stmt = (
            update(PriceHistoryIntraday)
            .where(
                PriceHistoryIntraday.close.isnot(None),
                PriceHistoryIntraday.close > 0,
                PriceHistoryIntraday.resolve_required == True
            )
            .values(resolve_required=False)
        )
        result = await session.execute(stmt)
        print(f'Cleared resolve_required for {result.rowcount} records with valid data')

        # Reset resolve_tries for records we want to retry
        # Focus on records in the last 24 hours that have NULL data
        now = datetime.now(timezone.utc)
        cutoff = now - timedelta(hours=24)

        stmt = (
            update(PriceHistoryIntraday)
            .where(
                PriceHistoryIntraday.datetime >= cutoff,
                PriceHistoryIntraday.datetime < now,
                (PriceHistoryIntraday.close.is_(None)) | (PriceHistoryIntraday.close == 0)
            )
            .values(resolve_tries=0, resolve_required=True)
        )
        result = await session.execute(stmt)
        print(f'Reset resolve_tries to 0 for {result.rowcount} records with NULL data in last 24h')

        await session.commit()
        print('Done!')
        break


if __name__ == "__main__":
    asyncio.run(reset_tries())

