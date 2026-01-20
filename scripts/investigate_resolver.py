"""
Script to investigate why data resolver is not finding records to resolve.
"""
import asyncio
import sys
import os

# Fix Unicode output on Windows
if sys.stdout:
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from datetime import datetime, timezone, timedelta
from sqlalchemy import select, func
from config.database_config import get_db_session
from models import PriceHistoryIntraday, Instrument


async def investigate():
    now = datetime.now(timezone.utc)
    cutoff_time = now - timedelta(hours=24)
    buffer_zone = now - timedelta(minutes=60)

    print(f'Now (UTC): {now}')
    print(f'Now (IST): {now.astimezone()}')
    print(f'Cutoff (24h ago): {cutoff_time}')
    print(f'Buffer zone (60m ago): {buffer_zone}')
    print()

    async for session in get_db_session():
        # Check total records
        stmt = select(func.count()).select_from(PriceHistoryIntraday)
        result = await session.execute(stmt)
        total_count = result.scalar()
        print(f'Total records in price_history_intraday: {total_count}')

        # Check records with resolve_required=True
        stmt = select(func.count()).select_from(PriceHistoryIntraday).where(
            PriceHistoryIntraday.resolve_required == True
        )
        result = await session.execute(stmt)
        resolve_required_count = result.scalar()
        print(f'Records with resolve_required=True: {resolve_required_count}')

        # Check records with NULL/0 OHLC
        stmt = select(func.count()).select_from(PriceHistoryIntraday).where(
            (PriceHistoryIntraday.open == 0) |
            (PriceHistoryIntraday.open.is_(None)) |
            (PriceHistoryIntraday.close == 0) |
            (PriceHistoryIntraday.close.is_(None))
        )
        result = await session.execute(stmt)
        null_ohlc_count = result.scalar()
        print(f'Records with NULL/0 OHLC: {null_ohlc_count}')

        # Check records in the 24h window (no buffer)
        stmt = select(func.count()).select_from(PriceHistoryIntraday).where(
            PriceHistoryIntraday.datetime >= cutoff_time,
            PriceHistoryIntraday.datetime < now
        )
        result = await session.execute(stmt)
        in_24h_window = result.scalar()
        print(f'Records in 24h window (no buffer): {in_24h_window}')

        # Check records in the 24h window WITH buffer
        stmt = select(func.count()).select_from(PriceHistoryIntraday).where(
            PriceHistoryIntraday.datetime >= cutoff_time,
            PriceHistoryIntraday.datetime < buffer_zone
        )
        result = await session.execute(stmt)
        in_window_count = result.scalar()
        print(f'Records in 24h window (WITH 60m buffer): {in_window_count}')

        # Check records with resolve_tries < 3
        stmt = select(func.count()).select_from(PriceHistoryIntraday).where(
            PriceHistoryIntraday.resolve_tries < 3
        )
        result = await session.execute(stmt)
        low_tries = result.scalar()
        print(f'Records with resolve_tries < 3: {low_tries}')

        # Check records needing resolution (full query)
        stmt = select(func.count()).select_from(PriceHistoryIntraday).where(
            PriceHistoryIntraday.datetime >= cutoff_time,
            PriceHistoryIntraday.datetime < buffer_zone,
            PriceHistoryIntraday.resolve_tries < 3,
            (PriceHistoryIntraday.resolve_required.is_(True)) |
            (PriceHistoryIntraday.open == 0) |
            (PriceHistoryIntraday.open.is_(None)) |
            (PriceHistoryIntraday.close == 0) |
            (PriceHistoryIntraday.close.is_(None))
        )
        result = await session.execute(stmt)
        needing_resolution = result.scalar()
        print(f'Records needing resolution (full query): {needing_resolution}')

        print()

        # Get sample of records with resolve_required=True
        stmt = select(
            PriceHistoryIntraday.id,
            PriceHistoryIntraday.instrument_id,
            PriceHistoryIntraday.datetime,
            PriceHistoryIntraday.resolve_required,
            PriceHistoryIntraday.resolve_tries,
            PriceHistoryIntraday.open,
            PriceHistoryIntraday.close
        ).where(
            PriceHistoryIntraday.resolve_required == True
        ).order_by(PriceHistoryIntraday.datetime.desc()).limit(5)
        result = await session.execute(stmt)
        samples = result.all()

        print('Sample records with resolve_required=True (most recent):')
        for s in samples:
            print(f'  ID={s.id}, inst={s.instrument_id}, dt={s.datetime}, tries={s.resolve_tries}, open={s.open}, close={s.close}')

        # Get NIFTY records specifically
        stmt = select(Instrument.id).where(Instrument.symbol == 'NIFTY')
        result = await session.execute(stmt)
        nifty_id = result.scalar()

        if nifty_id:
            print(f'\nNIFTY instrument ID: {nifty_id}')

            # Get NIFTY intraday records for today
            today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
            stmt = select(
                PriceHistoryIntraday.datetime,
                PriceHistoryIntraday.open,
                PriceHistoryIntraday.high,
                PriceHistoryIntraday.low,
                PriceHistoryIntraday.close,
                PriceHistoryIntraday.resolve_required,
                PriceHistoryIntraday.resolve_tries
            ).where(
                PriceHistoryIntraday.instrument_id == nifty_id,
                PriceHistoryIntraday.datetime >= today_start
            ).order_by(PriceHistoryIntraday.datetime)

            result = await session.execute(stmt)
            nifty_records = result.all()

            print(f'NIFTY records for today ({today_start.date()}): {len(nifty_records)}')
            print('\nNIFTY records details (first 10):')
            for i, r in enumerate(nifty_records[:10]):
                status = '✓' if r.close and r.close > 0 else '✗'
                print(f'  {status} {r.datetime} - O:{r.open} H:{r.high} L:{r.low} C:{r.close} resolve_req:{r.resolve_required} tries:{r.resolve_tries}')

            if len(nifty_records) > 10:
                print('  ...')
                print(f'\nNIFTY records (last 5):')
                for r in nifty_records[-5:]:
                    status = '✓' if r.close and r.close > 0 else '✗'
                    print(f'  {status} {r.datetime} - O:{r.open} H:{r.high} L:{r.low} C:{r.close} resolve_req:{r.resolve_required} tries:{r.resolve_tries}')

        break


if __name__ == "__main__":
    asyncio.run(investigate())

