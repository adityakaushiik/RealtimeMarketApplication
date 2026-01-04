"""
Deep Investigation: Dhan Provider Data Quality Analysis

This script performs a deep investigation into the Dhan provider data quality.
It will:
1. Subscribe to a test symbol and count raw ticks received
2. Compare our received data against what Dhan should be sending
3. Check for timestamp issues
4. Analyze the data flow from Dhan -> Redis -> Database

Run this during market hours for best results.
"""

import asyncio
import sys
import time
from datetime import datetime, timedelta, timezone, date
from collections import defaultdict
from dataclasses import dataclass, field
from typing import List, Dict
import json

sys.path.insert(0, r"C:\Users\tech\OneDrive\Documents\GitHub\RealtimeMarketApplication")

import pytz
from sqlalchemy import select, text
from sqlalchemy.orm import joinedload

from config.database_config import get_db_session
from config.redis_config import get_redis
from config.logger import logger
from models import Instrument, PriceHistoryIntraday, ProviderInstrumentMapping, Provider


@dataclass
class TickRecord:
    """Record of a single tick for analysis."""
    timestamp_ms: int
    price: float
    volume: float
    source: str = "unknown"  # 'dhan', 'redis', 'db'

    @property
    def datetime_utc(self) -> datetime:
        return datetime.fromtimestamp(self.timestamp_ms / 1000, tz=timezone.utc)

    @property
    def datetime_ist(self) -> datetime:
        ist = pytz.timezone('Asia/Kolkata')
        return self.datetime_utc.astimezone(ist)


@dataclass
class BucketAnalysis:
    """Analysis of a 5-minute bucket."""
    bucket_start_ms: int
    ticks: List[TickRecord] = field(default_factory=list)
    db_ohlcv: Dict = field(default_factory=dict)

    @property
    def tick_count(self) -> int:
        return len(self.ticks)

    @property
    def is_singular(self) -> bool:
        """Returns True if all ticks have the same price."""
        if not self.ticks:
            return True
        prices = [t.price for t in self.ticks]
        return len(set(prices)) == 1

    @property
    def computed_ohlcv(self) -> Dict:
        """Compute OHLCV from ticks."""
        if not self.ticks:
            return {}
        sorted_ticks = sorted(self.ticks, key=lambda x: x.timestamp_ms)
        return {
            'open': sorted_ticks[0].price,
            'high': max(t.price for t in self.ticks),
            'low': min(t.price for t in self.ticks),
            'close': sorted_ticks[-1].price,
            'volume': sum(t.volume for t in self.ticks),
        }


class DhanDataQualityAnalyzer:
    """Analyzes data quality from Dhan provider."""

    def __init__(self, symbols: List[str] = None):
        self.symbols = symbols or ["KOTAKBANK"]
        self.redis = None
        self.instruments = {}  # symbol -> Instrument
        self.provider_codes = {}  # symbol -> dhan_security_id
        self.tick_buffer = defaultdict(list)  # symbol -> [TickRecord]
        self.analysis_results = {}

    async def initialize(self):
        """Initialize and load instrument info."""
        self.redis = get_redis()

        async for session in get_db_session():
            for symbol in self.symbols:
                stmt = (
                    select(Instrument)
                    .options(joinedload(Instrument.exchange))
                    .where(Instrument.symbol == symbol)
                )
                result = await session.execute(stmt)
                instrument = result.scalar_one_or_none()

                if instrument:
                    self.instruments[symbol] = instrument

                    # Get Dhan mapping
                    stmt = (
                        select(ProviderInstrumentMapping.provider_instrument_search_code)
                        .join(Provider, ProviderInstrumentMapping.provider_id == Provider.id)
                        .where(
                            ProviderInstrumentMapping.instrument_id == instrument.id,
                            Provider.code == "DHAN"
                        )
                    )
                    result = await session.execute(stmt)
                    code = result.scalar_one_or_none()
                    if code:
                        self.provider_codes[symbol] = code

        print(f"✅ Initialized with {len(self.instruments)} instruments")
        for symbol in self.instruments:
            print(f"   {symbol} -> Dhan ID: {self.provider_codes.get(symbol, 'NOT FOUND')}")

        return len(self.instruments) > 0

    async def analyze_redis_raw_data(self, symbol: str, minutes_back: int = 60):
        """Analyze raw tick data from Redis TimeSeries."""
        print(f"\n{'='*60}")
        print(f"📊 REDIS RAW DATA ANALYSIS: {symbol}")
        print(f"{'='*60}")

        if symbol not in self.instruments:
            print(f"❌ Symbol {symbol} not initialized")
            return

        price_key = f"{symbol}:price"
        volume_key = f"{symbol}:volume"

        try:
            # Get raw samples from the last N minutes
            now_ms = int(time.time() * 1000)
            from_ms = now_ms - (minutes_back * 60 * 1000)

            # Get price samples
            price_samples = await self.redis.ts().range(
                price_key,
                from_time=from_ms,
                to_time=now_ms,
            )

            # Get volume samples
            try:
                volume_samples = await self.redis.ts().range(
                    volume_key,
                    from_time=from_ms,
                    to_time=now_ms,
                )
                volume_map = {ts: vol for ts, vol in volume_samples}
            except:
                volume_map = {}

            if not price_samples:
                print(f"❌ No samples found in Redis for the last {minutes_back} minutes")
                print("   This could mean:")
                print("   - Redis TimeSeries retention has expired (15 min default)")
                print("   - No data was ever written for this symbol")
                print("   - The symbol key might be different")
                return

            print(f"\n📈 Found {len(price_samples)} price samples")

            # Group by 5-minute buckets
            buckets = defaultdict(list)
            for ts, price in price_samples:
                bucket_ms = (ts // (5 * 60 * 1000)) * (5 * 60 * 1000)
                vol = volume_map.get(ts, 0)
                buckets[bucket_ms].append(TickRecord(
                    timestamp_ms=ts,
                    price=float(price),
                    volume=float(vol),
                    source='redis'
                ))

            print(f"📊 Data spans {len(buckets)} 5-minute buckets\n")

            # Analyze each bucket
            ist = pytz.timezone('Asia/Kolkata')

            singular_buckets = []
            multi_tick_buckets = []

            print("Bucket Analysis (5-minute intervals):")
            print("-" * 90)
            print(f"{'Time (IST)':<12} | {'Ticks':>6} | {'O':>10} | {'H':>10} | {'L':>10} | {'C':>10} | {'Status':<15}")
            print("-" * 90)

            for bucket_ms in sorted(buckets.keys()):
                ticks = buckets[bucket_ms]
                bucket_dt = datetime.fromtimestamp(bucket_ms / 1000, tz=timezone.utc).astimezone(ist)

                analysis = BucketAnalysis(bucket_start_ms=bucket_ms, ticks=ticks)
                ohlcv = analysis.computed_ohlcv

                status = ""
                if analysis.tick_count == 1:
                    status = "⚠️ 1 TICK ONLY"
                    singular_buckets.append(analysis)
                elif analysis.is_singular:
                    status = "⚠️ SAME PRICE"
                    singular_buckets.append(analysis)
                else:
                    status = "✅ OK"
                    multi_tick_buckets.append(analysis)

                print(f"{bucket_dt.strftime('%H:%M'):<12} | {analysis.tick_count:>6} | "
                      f"{ohlcv.get('open', 0):>10.2f} | {ohlcv.get('high', 0):>10.2f} | "
                      f"{ohlcv.get('low', 0):>10.2f} | {ohlcv.get('close', 0):>10.2f} | {status:<15}")

            # Summary
            print("\n" + "=" * 60)
            print("📊 SUMMARY")
            print("=" * 60)
            print(f"Total buckets analyzed: {len(buckets)}")
            print(f"✅ Normal buckets (multiple ticks, varying prices): {len(multi_tick_buckets)}")
            print(f"⚠️  Problem buckets (1 tick or same prices): {len(singular_buckets)}")

            if len(buckets) > 0:
                problem_pct = (len(singular_buckets) / len(buckets)) * 100
                print(f"\n⚠️  Problem rate: {problem_pct:.1f}%")

            # Tick distribution
            tick_counts = [len(ticks) for ticks in buckets.values()]
            if tick_counts:
                print(f"\n📈 Tick Distribution per 5-min bucket:")
                print(f"   Min: {min(tick_counts)}")
                print(f"   Max: {max(tick_counts)}")
                print(f"   Avg: {sum(tick_counts)/len(tick_counts):.1f}")

                # Expected ticks for a liquid stock
                print(f"\n💡 For a liquid Nifty 50 stock like {symbol}:")
                print(f"   Expected: ~50-200+ ticks per 5-min during active hours")
                print(f"   Actual average: {sum(tick_counts)/len(tick_counts):.1f}")

                if sum(tick_counts)/len(tick_counts) < 10:
                    print(f"\n🚨 CRITICAL: Tick rate is VERY LOW!")
                    print(f"   This strongly suggests the provider is not sending enough data.")

            return {
                'total_buckets': len(buckets),
                'problem_buckets': len(singular_buckets),
                'avg_ticks_per_bucket': sum(tick_counts)/len(tick_counts) if tick_counts else 0,
            }

        except Exception as e:
            print(f"❌ Error: {e}")
            import traceback
            traceback.print_exc()

    async def compare_redis_vs_database(self, symbol: str):
        """Compare Redis aggregation vs Database values."""
        print(f"\n{'='*60}")
        print(f"📊 REDIS vs DATABASE COMPARISON: {symbol}")
        print(f"{'='*60}")

        if symbol not in self.instruments:
            print(f"❌ Symbol {symbol} not initialized")
            return

        instrument = self.instruments[symbol]
        ist = pytz.timezone('Asia/Kolkata')

        # Get today's date range
        today = date.today()
        market_open = instrument.exchange.market_open_time
        market_close = instrument.exchange.market_close_time

        start_dt = ist.localize(datetime.combine(today, market_open))
        end_dt = ist.localize(datetime.combine(today, market_close))

        async for session in get_db_session():
            # Get database records
            stmt = (
                select(PriceHistoryIntraday)
                .where(
                    PriceHistoryIntraday.instrument_id == instrument.id,
                    PriceHistoryIntraday.datetime >= start_dt,
                    PriceHistoryIntraday.datetime <= end_dt,
                )
                .order_by(PriceHistoryIntraday.datetime)
            )
            result = await session.execute(stmt)
            db_records = result.scalars().all()

            if not db_records:
                print("❌ No database records for today")
                return

            print(f"\n📈 Found {len(db_records)} database records for today")

            # Count singular vs normal
            singular = [r for r in db_records if r.open and r.open == r.high == r.low == r.close]
            null_data = [r for r in db_records if r.open is None]
            normal = [r for r in db_records if r.open and not (r.open == r.high == r.low == r.close)]

            print(f"\n📊 Database Record Analysis:")
            print(f"   ✅ Normal OHLC: {len(normal)}")
            print(f"   ⚠️  Singular OHLC (O=H=L=C): {len(singular)}")
            print(f"   ❌ NULL data: {len(null_data)}")

            if singular:
                print(f"\n📍 First 10 Singular Records:")
                print("-" * 80)
                for r in singular[:10]:
                    local_time = r.datetime.astimezone(ist)
                    print(f"   {local_time.strftime('%H:%M')} | Price: {r.close:.2f} | Volume: {r.volume or 0}")

    async def check_dhan_subscription_details(self, symbol: str):
        """Check details about how we're subscribing to Dhan."""
        print(f"\n{'='*60}")
        print(f"📊 DHAN SUBSCRIPTION CHECK: {symbol}")
        print(f"{'='*60}")

        dhan_id = self.provider_codes.get(symbol)
        if not dhan_id:
            print(f"❌ No Dhan Security ID found for {symbol}")
            return

        print(f"\n🔍 Dhan Configuration:")
        print(f"   Security ID: {dhan_id}")
        print(f"   Exchange Segment: NSE_EQ (assumed)")

        print(f"\n📝 Subscription Details:")
        print(f"   Request Code: 17 (SUBSCRIBE_QUOTE)")
        print(f"   This mode provides: LTP, LTQ (Last Traded Qty), LTT, ATP, Volume")

        print(f"\n⚠️  Important Notes about Dhan Data:")
        print(f"""
   1. Quote Mode (Code 17) sends updates ONLY when a trade happens.
      - If no trades in a period, no data is sent.
      - For illiquid stocks, this causes gaps.
      
   2. Dhan sends timestamps in IST but encoded as Unix timestamps.
      - We subtract 19800 seconds (5.5 hours) to convert to UTC.
      - If this conversion is wrong, data goes to wrong buckets.
      
   3. WebSocket might miss data if:
      - Connection drops and reconnects
      - Network latency causes delayed delivery
      - Rate limiting kicks in
      
   4. For KOTAKBANK (highly liquid stock):
      - We should receive MANY ticks per second during market hours
      - If we're getting only 1-2 per 5 minutes, something is wrong
        """)

    async def run_full_analysis(self):
        """Run complete analysis on all symbols."""
        if not await self.initialize():
            print("❌ Failed to initialize")
            return

        for symbol in self.symbols:
            await self.analyze_redis_raw_data(symbol)
            await self.compare_redis_vs_database(symbol)
            await self.check_dhan_subscription_details(symbol)

        self.print_recommendations()

    def print_recommendations(self):
        """Print recommendations based on analysis."""
        print(f"\n{'='*60}")
        print(f"🎯 RECOMMENDATIONS")
        print(f"{'='*60}")

        print("""
Based on the analysis, here are the potential issues and fixes:

1. **IF LOW TICK COUNT** (< 10 ticks per 5-min for liquid stocks):
   
   LIKELY CAUSE: Dhan WebSocket not sending enough data
   
   POSSIBLE FIXES:
   a) Switch from Quote (17) to Full (21) subscription mode
      - Full mode includes market depth updates
      - More data points = more accurate OHLC
   
   b) Check if Dhan Security ID is correct
      - Verify against Dhan's instrument master
      - Wrong ID = no data or wrong stock's data
   
   c) Add heartbeat monitoring to detect connection drops
      - If WS drops, we miss ticks silently
      - Log reconnection events with timestamps

2. **IF TIMESTAMP ISSUES** (data in wrong buckets):
   
   LIKELY CAUSE: Timestamp conversion from IST to UTC is wrong
   
   CHECK: Verify the -19800 seconds offset in dhan_provider.py
   - Print raw timestamps vs converted to confirm

3. **IF REDIS RETENTION ISSUE** (data expires too fast):
   
   CURRENT: 15 minute retention
   
   FIX: Increase retention if you need to analyze older data
   - But for 5-min OHLC, 15 min should be sufficient

4. **IMMEDIATE DEBUG ACTIONS**:
   
   a) Add logging to count ticks received per symbol per minute
   b) Compare our tick count with broker terminal tick count
   c) Check Dhan WebSocket connection logs for errors
   d) Verify the internal symbol -> Dhan ID mapping is correct
        """)


async def main():
    """Main entry point."""
    print("=" * 60)
    print("🔍 DHAN DATA QUALITY DEEP ANALYSIS")
    print("=" * 60)
    print(f"Time: {datetime.now()}")
    print("=" * 60)

    analyzer = DhanDataQualityAnalyzer(symbols=["KOTAKBANK"])
    await analyzer.run_full_analysis()


if __name__ == "__main__":
    asyncio.run(main())

