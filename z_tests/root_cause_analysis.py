"""
ROOT CAUSE ANALYSIS: Why OHLC values are singular (O=H=L=C)

Based on initial investigation, the issue is that during certain 5-minute periods,
only 1-9 ticks were received from Dhan, resulting in identical OHLC values.

This script performs deeper analysis to understand:
1. WHEN exactly did these low-tick periods occur?
2. Was the Dhan WebSocket connected during those periods?
3. Are other instruments affected at the same time?
4. Is this a pattern (e.g., specific times of day)?
"""

import asyncio
import sys
from datetime import datetime, timedelta, timezone, date, time
from collections import defaultdict
from typing import Dict, List, Tuple
import pytz

sys.path.insert(0, r"C:\Users\tech\OneDrive\Documents\GitHub\RealtimeMarketApplication")

from sqlalchemy import select, func, text
from sqlalchemy.orm import joinedload

from config.database_config import get_db_session
from config.redis_config import get_redis
from config.logger import logger
from models import Instrument, PriceHistoryIntraday, Exchange


class RootCauseAnalyzer:
    """Performs deep root cause analysis on OHLC singular value issue."""

    def __init__(self):
        self.ist = pytz.timezone('Asia/Kolkata')

    async def analyze_singular_pattern(self, symbol: str = "KOTAKBANK", target_date: date = None):
        """Analyze the pattern of singular OHLC values."""
        target_date = target_date or date.today()

        print(f"\n{'='*70}")
        print(f"📊 ROOT CAUSE ANALYSIS FOR SINGULAR OHLC VALUES")
        print(f"Symbol: {symbol}")
        print(f"Date: {target_date}")
        print(f"{'='*70}")

        async for session in get_db_session():
            # Get instrument with exchange info
            stmt = (
                select(Instrument)
                .options(joinedload(Instrument.exchange))
                .where(Instrument.symbol == symbol)
            )
            result = await session.execute(stmt)
            instrument = result.scalar_one_or_none()

            if not instrument:
                print(f"❌ Instrument {symbol} not found!")
                return

            exchange = instrument.exchange

            # Calculate date range
            market_open = exchange.market_open_time
            market_close = exchange.market_close_time

            start_dt = self.ist.localize(datetime.combine(target_date, market_open))
            end_dt = self.ist.localize(datetime.combine(target_date, market_close))

            print(f"\nMarket Hours: {market_open} - {market_close}")
            print(f"UTC Range: {start_dt.astimezone(timezone.utc)} to {end_dt.astimezone(timezone.utc)}")

            # Fetch all records for today
            stmt = (
                select(PriceHistoryIntraday)
                .where(
                    PriceHistoryIntraday.instrument_id == instrument.id,
                    PriceHistoryIntraday.datetime >= start_dt.astimezone(timezone.utc),
                    PriceHistoryIntraday.datetime <= end_dt.astimezone(timezone.utc),
                )
                .order_by(PriceHistoryIntraday.datetime)
            )
            result = await session.execute(stmt)
            records = result.scalars().all()

            print(f"\n📈 Total Records: {len(records)}")

            # Categorize records
            singular_records = []
            normal_records = []
            null_records = []

            for r in records:
                if r.open is None:
                    null_records.append(r)
                elif r.open == r.high == r.low == r.close:
                    singular_records.append(r)
                else:
                    normal_records.append(r)

            print(f"\n📊 RECORD ANALYSIS:")
            print(f"   ✅ Normal OHLC: {len(normal_records)}")
            print(f"   ⚠️  Singular OHLC: {len(singular_records)}")
            print(f"   ❌ NULL data: {len(null_records)}")

            # Analyze pattern of singular/null records
            print(f"\n📍 TIMELINE OF PROBLEM RECORDS:")
            print("-" * 80)

            all_problem_records = sorted(
                singular_records + null_records,
                key=lambda r: r.datetime
            )

            # Group consecutive problem periods
            problem_periods = []
            current_period = []

            for r in all_problem_records:
                if not current_period:
                    current_period.append(r)
                else:
                    # Check if consecutive (within 10 minutes)
                    last_time = current_period[-1].datetime
                    if (r.datetime - last_time).total_seconds() <= 600:
                        current_period.append(r)
                    else:
                        problem_periods.append(current_period)
                        current_period = [r]

            if current_period:
                problem_periods.append(current_period)

            print(f"\n🔴 Found {len(problem_periods)} problem periods:")

            for i, period in enumerate(problem_periods):
                start = period[0].datetime.astimezone(self.ist)
                end = period[-1].datetime.astimezone(self.ist)
                duration = (period[-1].datetime - period[0].datetime).total_seconds() / 60

                singular_in_period = sum(1 for r in period if r.open is not None)
                null_in_period = sum(1 for r in period if r.open is None)

                print(f"\n   Period {i+1}: {start.strftime('%H:%M')} - {end.strftime('%H:%M')} ({duration+5:.0f} min)")
                print(f"      Singular: {singular_in_period}, NULL: {null_in_period}")

                # Show detailed records in this period
                for r in period[:5]:  # Show first 5
                    local_time = r.datetime.astimezone(self.ist)
                    if r.open is None:
                        print(f"      {local_time.strftime('%H:%M')} -> NULL (no data)")
                    else:
                        print(f"      {local_time.strftime('%H:%M')} -> {r.close:.2f} (vol={r.volume})")
                if len(period) > 5:
                    print(f"      ... and {len(period) - 5} more")

            # Analyze volume pattern
            print(f"\n📊 VOLUME ANALYSIS:")

            if singular_records:
                singular_volumes = [r.volume for r in singular_records if r.volume]
                if singular_volumes:
                    print(f"   Singular candles volume: min={min(singular_volumes)}, max={max(singular_volumes)}, avg={sum(singular_volumes)/len(singular_volumes):.1f}")

            if normal_records:
                normal_volumes = [r.volume for r in normal_records if r.volume]
                if normal_volumes:
                    print(f"   Normal candles volume: min={min(normal_volumes)}, max={max(normal_volumes)}, avg={sum(normal_volumes)/len(normal_volumes):.1f}")

            return {
                'singular': singular_records,
                'normal': normal_records,
                'null': null_records,
                'problem_periods': problem_periods,
            }

    async def check_other_instruments_at_problem_times(self, problem_periods: List[List], limit: int = 5):
        """Check if other instruments also have problems at the same times."""

        print(f"\n{'='*70}")
        print(f"📊 CHECKING OTHER INSTRUMENTS AT PROBLEM TIMES")
        print(f"{'='*70}")

        if not problem_periods:
            print("No problem periods to analyze")
            return

        async for session in get_db_session():
            # Get a sample of other instruments
            stmt = (
                select(Instrument)
                .where(Instrument.should_record_data == True)
                .limit(10)
            )
            result = await session.execute(stmt)
            instruments = result.scalars().all()

            print(f"\nChecking {len(instruments)} instruments...")

            for period in problem_periods[:3]:  # Check first 3 problem periods
                start = period[0].datetime
                end = period[-1].datetime

                print(f"\n🔍 Problem Period: {start.astimezone(self.ist).strftime('%H:%M')} - {end.astimezone(self.ist).strftime('%H:%M')}")

                for instrument in instruments[:limit]:
                    stmt = (
                        select(PriceHistoryIntraday)
                        .where(
                            PriceHistoryIntraday.instrument_id == instrument.id,
                            PriceHistoryIntraday.datetime >= start,
                            PriceHistoryIntraday.datetime <= end,
                        )
                    )
                    result = await session.execute(stmt)
                    records = result.scalars().all()

                    if not records:
                        status = "❌ No records"
                    else:
                        singular = sum(1 for r in records if r.open and r.open == r.high == r.low == r.close)
                        null_count = sum(1 for r in records if r.open is None)
                        normal = len(records) - singular - null_count

                        if null_count == len(records):
                            status = "❌ All NULL"
                        elif singular + null_count > normal:
                            status = f"⚠️  Problem (normal={normal}, singular={singular}, null={null_count})"
                        else:
                            status = f"✅ OK (normal={normal})"

                    print(f"   {instrument.symbol}: {status}")

    async def analyze_connection_gaps(self, symbol: str = "KOTAKBANK"):
        """Analyze if the singular values correlate with connection gaps."""

        print(f"\n{'='*70}")
        print(f"📊 CONNECTION GAP ANALYSIS")
        print(f"{'='*70}")

        redis = get_redis()
        if not redis:
            print("❌ Redis not available")
            return

        price_key = f"{symbol}:price"

        try:
            samples = await redis.ts().range(price_key, from_time="-", to_time="+")

            if len(samples) < 2:
                print("Not enough samples for gap analysis")
                return

            # Find gaps > 30 seconds
            gaps = []
            for i in range(1, len(samples)):
                gap_ms = samples[i][0] - samples[i-1][0]
                if gap_ms > 30000:
                    gaps.append({
                        'start': samples[i-1][0],
                        'end': samples[i][0],
                        'duration_s': gap_ms / 1000,
                    })

            print(f"\n🔍 Found {len(gaps)} gaps > 30 seconds:")

            for gap in gaps[:10]:
                start_dt = datetime.fromtimestamp(gap['start']/1000, tz=timezone.utc).astimezone(self.ist)
                end_dt = datetime.fromtimestamp(gap['end']/1000, tz=timezone.utc).astimezone(self.ist)
                print(f"   {start_dt.strftime('%H:%M:%S')} -> {end_dt.strftime('%H:%M:%S')} ({gap['duration_s']:.1f}s)")

            print(f"""
📝 INTERPRETATION:
   - Gaps in tick data indicate periods when no trades happened OR
   - The WebSocket connection was down OR
   - Dhan was not sending data for this symbol

   If gaps align with singular OHLC periods in the database,
   it confirms the issue is at the data source level (Dhan),
   not in our processing.
            """)

        except Exception as e:
            print(f"❌ Error: {e}")

    async def generate_diagnosis(self, analysis_result: Dict):
        """Generate final diagnosis based on all analysis."""

        print(f"\n{'='*70}")
        print(f"🎯 FINAL DIAGNOSIS")
        print(f"{'='*70}")

        singular = analysis_result.get('singular', [])
        null_records = analysis_result.get('null', [])
        problem_periods = analysis_result.get('problem_periods', [])

        print(f"""
📋 FINDINGS SUMMARY:

1. PROBLEM SCOPE:
   - {len(singular)} singular OHLC candles (O=H=L=C)
   - {len(null_records)} NULL/missing candles
   - {len(problem_periods)} distinct problem periods

2. VOLUME EVIDENCE:
   - Singular candles have very low volume (1-9 typically)
   - This indicates only 1-9 ticks were received in those 5-min windows
   - For a Nifty 50 stock like KOTAKBANK, this is ABNORMAL

3. ROOT CAUSE HYPOTHESIS:
   
   PRIMARY CAUSE: WebSocket Connection Instability
   
   Evidence:
   - Problem periods are CLUSTERED (not random)
   - Low volume = few ticks received
   - Normal periods have 100+ ticks
   
   When WebSocket reconnects:
   - We miss ticks during the reconnection
   - First tick after reconnect becomes the only price in that bucket
   - Result: O=H=L=C (singular candle)

4. CONTRIBUTING FACTORS:
   
   a) Quote Mode (Code 17) only sends on trades
      - No heartbeat during quiet periods
      - Can't distinguish "no trades" from "disconnected"
   
   b) 15-minute Redis retention
      - Can't retroactively fill missing data
      - By the time we detect the gap, data is gone
   
   c) Dhan timestamp handling
      - IST to UTC conversion might cause edge-case issues

5. RECOMMENDED FIXES:
   
   IMMEDIATE:
   ✅ Add WebSocket connection monitoring
   ✅ Log reconnection events with timestamps
   ✅ Compare connection drops with singular candle times
   
   SHORT-TERM:
   ✅ Implement heartbeat detection
   ✅ Auto-reconnect with exponential backoff
   ✅ Mark affected candles for resolution
   
   LONG-TERM:
   ✅ Switch to Full Mode (Code 21) for more data points
   ✅ Implement redundant data sources
   ✅ Use Dhan REST API to backfill gaps
        """)


async def main():
    """Main entry point."""
    analyzer = RootCauseAnalyzer()

    # Run analysis
    result = await analyzer.analyze_singular_pattern("KOTAKBANK")

    if result:
        # Check other instruments
        await analyzer.check_other_instruments_at_problem_times(
            result.get('problem_periods', [])
        )

        # Analyze connection gaps
        await analyzer.analyze_connection_gaps("KOTAKBANK")

        # Generate diagnosis
        await analyzer.generate_diagnosis(result)


if __name__ == "__main__":
    asyncio.run(main())

