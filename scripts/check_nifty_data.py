"""Check NIFTY data state"""
import asyncio
import sys
import os

if sys.stdout:
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from datetime import datetime, timezone, timedelta
from sqlalchemy import select
from config.database_config import get_db_session
from models import PriceHistoryIntraday, Instrument


async def check_nifty():
    async for session in get_db_session():
        # Get NIFTY ID
        stmt = select(Instrument.id).where(Instrument.symbol == 'NIFTY')
        result = await session.execute(stmt)
        nifty_id = result.scalar()

        print(f'NIFTY ID: {nifty_id}')

        now = datetime.now(timezone.utc)
        today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)

        # Get all NIFTY records for today with NULL close
        stmt = select(
            PriceHistoryIntraday.datetime,
            PriceHistoryIntraday.close,
            PriceHistoryIntraday.resolve_required,
            PriceHistoryIntraday.resolve_tries
        ).where(
            PriceHistoryIntraday.instrument_id == nifty_id,
            PriceHistoryIntraday.datetime >= today_start,
            (PriceHistoryIntraday.close.is_(None)) | (PriceHistoryIntraday.close == 0)
        ).order_by(PriceHistoryIntraday.datetime)

        result = await session.execute(stmt)
        null_records = result.all()

        print(f'NIFTY records with NULL close for today: {len(null_records)}')
        for r in null_records[:20]:
            print(f'  {r.datetime} close={r.close} req={r.resolve_required} tries={r.resolve_tries}')

        # Check records with valid data
        stmt = select(
            PriceHistoryIntraday.datetime,
            PriceHistoryIntraday.close,
        ).where(
            PriceHistoryIntraday.instrument_id == nifty_id,
            PriceHistoryIntraday.datetime >= today_start,
            PriceHistoryIntraday.close.isnot(None),
            PriceHistoryIntraday.close > 0
        ).order_by(PriceHistoryIntraday.datetime)

        result = await session.execute(stmt)
        valid_records = result.all()
        print(f'\nNIFTY records with VALID close for today: {len(valid_records)}')
        if valid_records:
            print(f'  First: {valid_records[0].datetime} close={valid_records[0].close}')
            print(f'  Last: {valid_records[-1].datetime} close={valid_records[-1].close}')

        break


if __name__ == "__main__":
    asyncio.run(check_nifty())

