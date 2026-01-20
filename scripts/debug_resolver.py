"""
Debug the resolver - check what it's looking for vs what exists.
"""
import asyncio
import sys
import os
from datetime import datetime, timezone, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy import select, func
from config.database_config import get_db_session
from models.instruments import Instrument
from models.price_history_intraday import PriceHistoryIntraday


async def main():
    print("=" * 80)
    print("Resolver Debug - What records need resolution?")
    print("=" * 80)

    now = datetime.now(timezone.utc)
    cutoff_time = now - timedelta(hours=24)
    buffer_zone = now - timedelta(minutes=60)

    print(f"\nCurrent time (UTC): {now}")
    print(f"Cutoff (24h ago): {cutoff_time}")
    print(f"Buffer zone (60m ago): {buffer_zone}")
    print(f"\nResolver will look for records WHERE:")
    print(f"  - datetime >= {cutoff_time}")
    print(f"  - datetime < {buffer_zone}")
    print(f"  - resolve_tries < 3")
    print(f"  - resolve_required=True OR any OHLC is 0/NULL")

    async for session in get_db_session():
        # Get NIFTY
        result = await session.execute(
            select(Instrument).where(Instrument.symbol == "NIFTY")
        )
        nifty = result.scalar_one_or_none()

        if not nifty:
            print("NIFTY not found!")
            return

        print(f"\n\nNIFTY (instrument_id={nifty.id}) Analysis:")
        print("=" * 60)

        # Count all records in the time window
        result = await session.execute(
            select(func.count()).select_from(PriceHistoryIntraday)
            .where(
                PriceHistoryIntraday.instrument_id == nifty.id,
                PriceHistoryIntraday.datetime >= cutoff_time,
            )
        )
        total_records = result.scalar()
        print(f"\n1. Total NIFTY records in last 24h: {total_records}")

        # Count records in buffer zone (excluded from resolution)
        result = await session.execute(
            select(func.count()).select_from(PriceHistoryIntraday)
            .where(
                PriceHistoryIntraday.instrument_id == nifty.id,
                PriceHistoryIntraday.datetime >= buffer_zone,
            )
        )
        buffer_records = result.scalar()
        print(f"2. Records in buffer zone (last 60m, excluded): {buffer_records}")

        # Count records that could potentially be resolved (outside buffer)
        result = await session.execute(
            select(func.count()).select_from(PriceHistoryIntraday)
            .where(
                PriceHistoryIntraday.instrument_id == nifty.id,
                PriceHistoryIntraday.datetime >= cutoff_time,
                PriceHistoryIntraday.datetime < buffer_zone,
            )
        )
        resolvable_records = result.scalar()
        print(f"3. Records eligible for resolution (outside buffer): {resolvable_records}")

        # Count records that actually need resolution
        result = await session.execute(
            select(func.count()).select_from(PriceHistoryIntraday)
            .where(
                PriceHistoryIntraday.instrument_id == nifty.id,
                PriceHistoryIntraday.datetime >= cutoff_time,
                PriceHistoryIntraday.datetime < buffer_zone,
                PriceHistoryIntraday.resolve_tries < 3,
                (PriceHistoryIntraday.resolve_required.is_(True))
                | (PriceHistoryIntraday.open == 0)
                | (PriceHistoryIntraday.open.is_(None))
                | (PriceHistoryIntraday.high == 0)
                | (PriceHistoryIntraday.high.is_(None))
                | (PriceHistoryIntraday.low == 0)
                | (PriceHistoryIntraday.low.is_(None))
                | (PriceHistoryIntraday.close == 0)
                | (PriceHistoryIntraday.close.is_(None)),
            )
        )
        need_resolution = result.scalar()
        print(f"4. Records that need resolution: {need_resolution}")

        # Show sample of records that need resolution
        if need_resolution > 0:
            result = await session.execute(
                select(PriceHistoryIntraday)
                .where(
                    PriceHistoryIntraday.instrument_id == nifty.id,
                    PriceHistoryIntraday.datetime >= cutoff_time,
                    PriceHistoryIntraday.datetime < buffer_zone,
                    PriceHistoryIntraday.resolve_tries < 3,
                    (PriceHistoryIntraday.resolve_required.is_(True))
                    | (PriceHistoryIntraday.open == 0)
                    | (PriceHistoryIntraday.open.is_(None))
                    | (PriceHistoryIntraday.high == 0)
                    | (PriceHistoryIntraday.high.is_(None))
                    | (PriceHistoryIntraday.low == 0)
                    | (PriceHistoryIntraday.low.is_(None))
                    | (PriceHistoryIntraday.close == 0)
                    | (PriceHistoryIntraday.close.is_(None)),
                )
                .limit(5)
            )
            records = result.scalars().all()
            print("\n   Sample records needing resolution:")
            for r in records:
                print(f"     {r.datetime} - O:{r.open} H:{r.high} L:{r.low} C:{r.close} resolve_required:{r.resolve_required}")

        # Show first and last records in the window
        print("\n5. Time range of NIFTY records in window:")
        result = await session.execute(
            select(
                func.min(PriceHistoryIntraday.datetime).label("min_dt"),
                func.max(PriceHistoryIntraday.datetime).label("max_dt"),
            )
            .where(
                PriceHistoryIntraday.instrument_id == nifty.id,
                PriceHistoryIntraday.datetime >= cutoff_time,
            )
        )
        row = result.one()
        print(f"   Earliest: {row.min_dt}")
        print(f"   Latest: {row.max_dt}")

        # Check if there are gaps in the data
        print("\n6. Checking for gaps in today's data:")
        today_start = datetime(2026, 1, 21, 3, 45, 0, tzinfo=timezone.utc)  # 9:15 IST
        today_end = datetime(2026, 1, 21, 10, 0, 0, tzinfo=timezone.utc)  # 15:30 IST

        result = await session.execute(
            select(PriceHistoryIntraday.datetime)
            .where(
                PriceHistoryIntraday.instrument_id == nifty.id,
                PriceHistoryIntraday.datetime >= today_start,
                PriceHistoryIntraday.datetime <= today_end,
            )
            .order_by(PriceHistoryIntraday.datetime)
        )
        timestamps = [r[0] for r in result.all()]

        if timestamps:
            print(f"   Today's records: {len(timestamps)}")
            print(f"   First: {timestamps[0]}")
            print(f"   Last: {timestamps[-1]}")

            # Check for 5-minute gaps
            gaps = []
            for i in range(1, len(timestamps)):
                diff = (timestamps[i] - timestamps[i-1]).total_seconds()
                if diff > 300:  # More than 5 minutes
                    gaps.append((timestamps[i-1], timestamps[i], diff/60))

            if gaps:
                print(f"   Found {len(gaps)} gaps:")
                for prev, next_, minutes in gaps[:5]:
                    print(f"     Gap from {prev} to {next_} ({minutes:.0f} minutes)")
            else:
                print("   No gaps found in 5-minute data ✅")
        else:
            print("   No records found for today's trading session!")

        break


if __name__ == "__main__":
    asyncio.run(main())

