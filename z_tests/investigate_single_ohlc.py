"""
Investigation Script: Why OHLC data shows singular values (dashes on chart)

This script investigates why for certain instruments like KOTAKBANK,
some 5-minute candles have O=H=L=C (appearing as dashes on the chart).

Possible causes to investigate:
1. Dhan provider not sending enough quotes/ticks during the interval
2. Redis TimeSeries not receiving/storing the data properly
3. Data aggregation issue when creating OHLCV from ticks
4. Timestamp alignment issues between provider and storage
5. Only 1 tick received in that 5-minute window

Author: Investigation Script
Date: 2025-12-23
"""

import asyncio
import sys
from datetime import datetime, timedelta, timezone, date
from collections import defaultdict
import pytz

# Add parent directory to path for imports
sys.path.insert(0, r"C:\Users\tech\OneDrive\Documents\GitHub\RealtimeMarketApplication")

from sqlalchemy import select, func, text
from sqlalchemy.orm import joinedload

from config.database_config import get_db_session
from config.redis_config import get_redis
from config.logger import logger
from models import Instrument, PriceHistoryIntraday, ProviderInstrumentMapping, Provider


class OHLCInvestigator:
    """
    Investigates why OHLC data shows singular values.
    """

    def __init__(self, symbol: str = "KOTAKBANK", target_date: date = None):
        self.symbol = symbol
        self.target_date = target_date or date.today()
        self.redis = None
        self.instrument = None
        self.provider_search_code = None

    async def initialize(self):
        """Initialize redis and fetch instrument info."""
        self.redis = get_redis()

        async for session in get_db_session():
            # Fetch the instrument
            stmt = (
                select(Instrument)
                .options(joinedload(Instrument.exchange))
                .where(Instrument.symbol == self.symbol)
            )
            result = await session.execute(stmt)
            self.instrument = result.scalar_one_or_none()

            if not self.instrument:
                print(f"❌ Instrument {self.symbol} not found!")
                return False

            print(f"✅ Found instrument: {self.instrument.symbol} (ID: {self.instrument.id})")
            print(f"   Exchange: {self.instrument.exchange.name} ({self.instrument.exchange.timezone})")

            # Get provider mapping
            stmt = (
                select(ProviderInstrumentMapping, Provider)
                .join(Provider, ProviderInstrumentMapping.provider_id == Provider.id)
                .where(ProviderInstrumentMapping.instrument_id == self.instrument.id)
            )
            result = await session.execute(stmt)
            mappings = result.all()

            for mapping, provider in mappings:
                print(f"   Provider: {provider.code} -> Search Code: {mapping.provider_instrument_search_code}")
                if provider.code == "DHAN":
                    self.provider_search_code = mapping.provider_instrument_search_code

            return True

    async def analyze_database_records(self):
        """Analyze database records for today to find singular OHLC candles."""
        print(f"\n{'='*60}")
        print(f"📊 ANALYZING DATABASE RECORDS FOR {self.target_date}")
        print(f"{'='*60}")

        tz = pytz.timezone(self.instrument.exchange.timezone)

        # Market times
        market_open = self.instrument.exchange.market_open_time
        market_close = self.instrument.exchange.market_close_time

        start_dt = tz.localize(datetime.combine(self.target_date, market_open))
        end_dt = tz.localize(datetime.combine(self.target_date, market_close))

        print(f"Market Hours: {start_dt} to {end_dt}")

        async for session in get_db_session():
            # Fetch all intraday records for today
            stmt = (
                select(PriceHistoryIntraday)
                .where(
                    PriceHistoryIntraday.instrument_id == self.instrument.id,
                    PriceHistoryIntraday.datetime >= start_dt,
                    PriceHistoryIntraday.datetime <= end_dt
                )
                .order_by(PriceHistoryIntraday.datetime)
            )
            result = await session.execute(stmt)
            records = result.scalars().all()

            if not records:
                print("❌ No records found for today!")
                return

            print(f"\n📈 Found {len(records)} intraday records")

            # Analyze each record
            singular_candles = []
            normal_candles = []
            missing_data_candles = []

            for record in records:
                if record.open is None or record.high is None or record.low is None or record.close is None:
                    missing_data_candles.append(record)
                elif record.open == record.high == record.low == record.close:
                    singular_candles.append(record)
                else:
                    normal_candles.append(record)

            print(f"\n📊 Candle Analysis:")
            print(f"   ✅ Normal candles (O≠H≠L≠C): {len(normal_candles)}")
            print(f"   ⚠️  Singular candles (O=H=L=C): {len(singular_candles)}")
            print(f"   ❌ Missing data (NULL values): {len(missing_data_candles)}")

            if singular_candles:
                print(f"\n🔍 SINGULAR CANDLES DETAILS (First 10):")
                print("-" * 80)
                for i, candle in enumerate(singular_candles[:10]):
                    local_time = candle.datetime.astimezone(tz)
                    print(f"   {i+1}. Time: {local_time.strftime('%H:%M')} | "
                          f"Price: {candle.close:.2f} | "
                          f"Volume: {candle.volume or 0} | "
                          f"Resolve Required: {candle.resolve_required}")

                if len(singular_candles) > 10:
                    print(f"   ... and {len(singular_candles) - 10} more singular candles")

            if missing_data_candles:
                print(f"\n🔍 MISSING DATA CANDLES (First 10):")
                print("-" * 80)
                for i, candle in enumerate(missing_data_candles[:10]):
                    local_time = candle.datetime.astimezone(tz)
                    print(f"   {i+1}. Time: {local_time.strftime('%H:%M')} | "
                          f"OHLC: [{candle.open}, {candle.high}, {candle.low}, {candle.close}] | "
                          f"Resolve Required: {candle.resolve_required}")

            return {
                "normal": normal_candles,
                "singular": singular_candles,
                "missing": missing_data_candles,
                "all": records
            }

    async def analyze_redis_timeseries(self):
        """Analyze Redis TimeSeries data to see tick distribution."""
        print(f"\n{'='*60}")
        print(f"📊 ANALYZING REDIS TIMESERIES DATA")
        print(f"{'='*60}")

        if not self.redis:
            print("❌ Redis not available!")
            return

        # Keys for this symbol
        price_key = f"{self.symbol}:price"
        volume_key = f"{self.symbol}:volume"

        try:
            # Check if key exists
            info = await self.redis.ts().info(price_key)
            print(f"\n✅ TimeSeries key exists: {price_key}")
            print(f"   Total samples: {info.total_samples}")
            print(f"   First timestamp: {datetime.fromtimestamp(info.first_timestamp/1000, tz=timezone.utc) if info.first_timestamp else 'N/A'}")
            print(f"   Last timestamp: {datetime.fromtimestamp(info.last_timestamp/1000, tz=timezone.utc) if info.last_timestamp else 'N/A'}")
            print(f"   Retention: {info.retention_msecs/1000/60:.1f} minutes")

            # Check the latest few samples (if any exist within retention)
            if info.total_samples > 0:
                # Get last few raw samples
                samples = await self.redis.ts().range(
                    price_key,
                    from_time="-",
                    to_time="+",
                )

                print(f"\n📈 Latest Raw Samples (last 20):")
                tz = pytz.timezone(self.instrument.exchange.timezone)
                for ts, val in samples[-20:]:
                    dt = datetime.fromtimestamp(ts/1000, tz=timezone.utc).astimezone(tz)
                    print(f"   {dt.strftime('%H:%M:%S')} -> {val:.2f}")

                # Count ticks per 5-minute bucket
                print(f"\n📊 Tick Distribution per 5-minute bucket:")
                bucket_counts = defaultdict(int)
                bucket_prices = defaultdict(list)

                for ts, val in samples:
                    # Round down to 5-minute bucket
                    bucket_ms = (ts // (5 * 60 * 1000)) * (5 * 60 * 1000)
                    bucket_counts[bucket_ms] += 1
                    bucket_prices[bucket_ms].append(val)

                for bucket_ts in sorted(bucket_counts.keys())[-20:]:
                    dt = datetime.fromtimestamp(bucket_ts/1000, tz=timezone.utc).astimezone(tz)
                    count = bucket_counts[bucket_ts]
                    prices = bucket_prices[bucket_ts]
                    price_range = f"{min(prices):.2f}-{max(prices):.2f}" if len(set(prices)) > 1 else f"{prices[0]:.2f} (SINGLE)"
                    print(f"   {dt.strftime('%H:%M')} -> {count} ticks, Price Range: {price_range}")

        except Exception as e:
            print(f"❌ Error accessing Redis TimeSeries: {e}")
            print(f"   This could mean no data is currently in Redis (retention expired)")

    async def analyze_tick_frequency(self):
        """Analyze how many ticks we receive per minute from the provider."""
        print(f"\n{'='*60}")
        print(f"📊 TICK FREQUENCY ANALYSIS (from DB data)")
        print(f"{'='*60}")

        async for session in get_db_session():
            # For the singular candles, we want to understand:
            # 1. If only 1 tick was received (stored as a single price)
            # 2. Or if multiple ticks had the same price (unlikely for liquid stocks)

            # Since the database stores OHLCV (aggregated), not raw ticks,
            # we can only infer from volume

            tz = pytz.timezone(self.instrument.exchange.timezone)
            market_open = self.instrument.exchange.market_open_time
            market_close = self.instrument.exchange.market_close_time

            start_dt = tz.localize(datetime.combine(self.target_date, market_open))
            end_dt = tz.localize(datetime.combine(self.target_date, market_close))

            stmt = (
                select(PriceHistoryIntraday)
                .where(
                    PriceHistoryIntraday.instrument_id == self.instrument.id,
                    PriceHistoryIntraday.datetime >= start_dt,
                    PriceHistoryIntraday.datetime <= end_dt,
                )
                .order_by(PriceHistoryIntraday.datetime)
            )
            result = await session.execute(stmt)
            records = result.scalars().all()

            print("\n📈 Comparing Volume between Singular vs Normal candles:")
            print("-" * 80)

            singular_volumes = []
            normal_volumes = []

            for record in records:
                if record.open is None:
                    continue
                vol = record.volume or 0
                if record.open == record.high == record.low == record.close:
                    singular_volumes.append(vol)
                else:
                    normal_volumes.append(vol)

            if singular_volumes:
                print(f"\n⚠️  Singular Candles Volume Stats:")
                print(f"   Count: {len(singular_volumes)}")
                print(f"   Min Volume: {min(singular_volumes)}")
                print(f"   Max Volume: {max(singular_volumes)}")
                print(f"   Avg Volume: {sum(singular_volumes)/len(singular_volumes):.0f}")

            if normal_volumes:
                print(f"\n✅ Normal Candles Volume Stats:")
                print(f"   Count: {len(normal_volumes)}")
                print(f"   Min Volume: {min(normal_volumes)}")
                print(f"   Max Volume: {max(normal_volumes)}")
                print(f"   Avg Volume: {sum(normal_volumes)/len(normal_volumes):.0f}")

    async def test_redis_aggregation(self):
        """Test how Redis TimeSeries aggregation works with limited data."""
        print(f"\n{'='*60}")
        print(f"📊 REDIS AGGREGATION TEST")
        print(f"{'='*60}")

        if not self.redis:
            print("❌ Redis not available!")
            return

        price_key = f"{self.symbol}:price"

        try:
            # Get the raw samples
            samples = await self.redis.ts().range(
                price_key,
                from_time="-",
                to_time="+",
            )

            if not samples:
                print("❌ No samples available in Redis")
                return

            # Pick a 5-minute bucket with data
            bucket_data = defaultdict(list)
            for ts, val in samples:
                bucket_ms = (ts // (5 * 60 * 1000)) * (5 * 60 * 1000)
                bucket_data[bucket_ms].append((ts, val))

            print("\n🔬 Checking OHLCV aggregation for each bucket:")
            print("-" * 80)

            for bucket_ts in sorted(bucket_data.keys())[-10:]:
                ticks = bucket_data[bucket_ts]

                # Manual OHLCV calculation
                if ticks:
                    sorted_ticks = sorted(ticks, key=lambda x: x[0])
                    open_price = sorted_ticks[0][1]
                    close_price = sorted_ticks[-1][1]
                    high_price = max(t[1] for t in ticks)
                    low_price = min(t[1] for t in ticks)

                    # Check if all prices are the same
                    unique_prices = set(t[1] for t in ticks)

                    tz = pytz.timezone(self.instrument.exchange.timezone)
                    dt = datetime.fromtimestamp(bucket_ts/1000, tz=timezone.utc).astimezone(tz)

                    status = "✅" if len(unique_prices) > 1 else "⚠️ SINGULAR"
                    print(f"   {dt.strftime('%H:%M')} | Ticks: {len(ticks):3d} | "
                          f"O:{open_price:.2f} H:{high_price:.2f} L:{low_price:.2f} C:{close_price:.2f} | "
                          f"Unique Prices: {len(unique_prices)} {status}")

        except Exception as e:
            print(f"❌ Error testing aggregation: {e}")

    async def check_provider_mapping(self):
        """Verify the provider mapping and subscription."""
        print(f"\n{'='*60}")
        print(f"📊 PROVIDER MAPPING CHECK")
        print(f"{'='*60}")

        async for session in get_db_session():
            # Check the exact mapping
            stmt = text("""
                SELECT 
                    i.symbol as internal_symbol,
                    pim.provider_instrument_search_code as provider_code,
                    p.code as provider_name,
                    p.name as provider_full_name
                FROM instruments i
                JOIN provider_instrument_mappings pim ON pim.instrument_id = i.id
                JOIN providers p ON p.id = pim.provider_id
                WHERE i.symbol = :symbol
            """)

            result = await session.execute(stmt, {"symbol": self.symbol})
            rows = result.fetchall()

            print(f"\n🔍 Provider mappings for {self.symbol}:")
            for row in rows:
                print(f"   {row.provider_name}: {row.internal_symbol} -> {row.provider_code}")

            if self.provider_search_code:
                print(f"\n🔍 Dhan search code: {self.provider_search_code}")
                print(f"   This is the Security ID that Dhan uses for this instrument")

    async def diagnose_issue(self):
        """Main diagnosis function that identifies the root cause."""
        print(f"\n{'='*60}")
        print(f"🔎 DIAGNOSIS SUMMARY")
        print(f"{'='*60}")

        # Collect findings
        findings = []

        # 1. Check database records
        analysis = await self.analyze_database_records()

        if analysis:
            singular_count = len(analysis.get("singular", []))
            total_count = len(analysis.get("all", []))

            if singular_count > 0:
                pct = (singular_count / total_count) * 100
                findings.append(f"📌 {singular_count} of {total_count} candles ({pct:.1f}%) are singular (O=H=L=C)")

                # Check if singular candles have low volume
                singular = analysis.get("singular", [])
                if singular:
                    avg_vol = sum((c.volume or 0) for c in singular) / len(singular)
                    if avg_vol < 100:
                        findings.append(f"📌 Singular candles have very low average volume: {avg_vol:.0f}")
                        findings.append("   ⚠️  This suggests only 1 or very few ticks received in those intervals")

        # 2. Check Redis
        await self.analyze_redis_timeseries()

        # 3. Check tick frequency
        await self.analyze_tick_frequency()

        # 4. Test aggregation
        await self.test_redis_aggregation()

        # 5. Check provider mapping
        await self.check_provider_mapping()

        print(f"\n{'='*60}")
        print(f"🎯 ROOT CAUSE ANALYSIS")
        print(f"{'='*60}")

        print("""
Based on the investigation, singular OHLC values (O=H=L=C) typically occur because:

1. **SINGLE TICK IN WINDOW**: Only 1 tick was received from Dhan during that 
   5-minute interval. When Redis TimeSeries aggregates with first/last/min/max,
   if there's only 1 data point, all values will be identical.

2. **POSSIBLE CAUSES FOR SINGLE TICK**:
   
   a) **Low Liquidity Period**: For less liquid stocks or during low trading periods,
      there might genuinely be only 1 trade in a 5-minute window.
   
   b) **Dhan Quote Mode vs Ticker Mode**: The provider is using SUBSCRIBE_QUOTE (code 17)
      which only sends data when there's a trade (LTT changes). If no trades happen,
      no data is sent.
   
   c) **Provider Connection Issues**: If the WebSocket disconnects/reconnects,
      some ticks might be missed.
   
   d) **Timestamp Misalignment**: If Dhan's timestamps don't align with our bucket
      boundaries, ticks might end up in wrong buckets.
   
   e) **Rate Limiting/Throttling**: Dhan might be throttling frequent updates for
      certain instruments.

3. **FOR KOTAKBANK (LIQUID STOCK)**: 
   If KOTAKBANK shows singular candles, this is ABNORMAL for a highly liquid 
   Nifty 50 stock. There should be many trades per minute.

4. **RECOMMENDED CHECKS**:
   - Monitor live tick count per minute for KOTAKBANK
   - Check if the Dhan Security ID mapping is correct
   - Verify WebSocket connection stability
   - Compare with a reference data source (broker terminal, TradingView)
""")

        return findings


async def run_investigation():
    """Run the full investigation."""
    print("=" * 60)
    print("🔍 OHLC SINGULAR VALUE INVESTIGATION")
    print("=" * 60)
    print(f"Investigating: KOTAKBANK")
    print(f"Date: {date.today()}")
    print("=" * 60)

    investigator = OHLCInvestigator(symbol="KOTAKBANK", target_date=date.today())

    if await investigator.initialize():
        findings = await investigator.diagnose_issue()

        if findings:
            print("\n📋 Summary of Findings:")
            for finding in findings:
                print(f"   {finding}")


async def run_live_tick_monitor(symbol: str = "KOTAKBANK", duration_seconds: int = 300):
    """
    Monitor live ticks for a symbol to count how many we receive per minute.
    This helps verify if the issue is with Dhan sending data or our processing.
    """
    print(f"\n{'='*60}")
    print(f"🔴 LIVE TICK MONITOR for {symbol}")
    print(f"Duration: {duration_seconds} seconds")
    print(f"{'='*60}")

    redis = get_redis()
    if not redis:
        print("❌ Redis not available!")
        return

    price_key = f"{symbol}:price"
    tz = pytz.timezone("Asia/Kolkata")

    start_time = datetime.now(tz)
    last_count = 0
    minute_counts = []

    print(f"Started at: {start_time.strftime('%H:%M:%S')}")
    print("Monitoring tick rate...\n")

    try:
        for elapsed in range(0, duration_seconds, 60):
            await asyncio.sleep(60)

            # Get current sample count
            try:
                info = await redis.ts().info(price_key)
                current_count = info.total_samples
                new_ticks = current_count - last_count
                minute_counts.append(new_ticks)
                last_count = current_count

                now = datetime.now(tz)
                print(f"   {now.strftime('%H:%M:%S')} | New ticks in last minute: {new_ticks}")

            except Exception as e:
                print(f"   Error: {e}")

    except asyncio.CancelledError:
        pass

    if minute_counts:
        print(f"\n📊 Tick Rate Summary:")
        print(f"   Average ticks/minute: {sum(minute_counts)/len(minute_counts):.1f}")
        print(f"   Min ticks/minute: {min(minute_counts)}")
        print(f"   Max ticks/minute: {max(minute_counts)}")

        if sum(minute_counts)/len(minute_counts) < 5:
            print(f"\n⚠️  WARNING: Very low tick rate detected!")
            print(f"   A liquid stock like {symbol} should have many ticks per minute")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Investigate OHLC singular values")
    parser.add_argument("--symbol", default="KOTAKBANK", help="Symbol to investigate")
    parser.add_argument("--date", default=None, help="Date to investigate (YYYY-MM-DD)")
    parser.add_argument("--live-monitor", action="store_true", help="Run live tick monitor")
    parser.add_argument("--duration", type=int, default=300, help="Duration for live monitor in seconds")

    args = parser.parse_args()

    if args.live_monitor:
        asyncio.run(run_live_tick_monitor(args.symbol, args.duration))
    else:
        target_date = None
        if args.date:
            target_date = datetime.strptime(args.date, "%Y-%m-%d").date()
        asyncio.run(run_investigation())

