"""
Real-Time Tick Counter and Data Quality Monitor

This script monitors the live data stream to count ticks and detect issues
in real-time. It helps identify:
1. How many ticks we're receiving per instrument per minute
2. Gaps in data delivery
3. Timestamp distribution issues
4. Connection stability

Run this during market hours for meaningful results.
"""

import asyncio
import sys
import time
from datetime import datetime, timezone
from collections import defaultdict
from typing import Dict, List
import pytz

sys.path.insert(0, r"C:\Users\tech\OneDrive\Documents\GitHub\RealtimeMarketApplication")

from config.logger import logger
from config.redis_config import get_redis
from utils.common_constants import DataIngestionFormat


class TickMonitor:
    """
    Monitors incoming ticks and collects statistics.
    Can be injected as the callback for the data ingestion system.
    """

    def __init__(self):
        self.ticks_per_minute: Dict[str, Dict[int, int]] = defaultdict(lambda: defaultdict(int))
        self.ticks_per_symbol: Dict[str, int] = defaultdict(int)
        self.last_tick_time: Dict[str, int] = {}
        self.price_changes: Dict[str, List[float]] = defaultdict(list)
        self.start_time = time.time()
        self.ist = pytz.timezone('Asia/Kolkata')

    def record_tick(self, message: DataIngestionFormat):
        """Record a tick for analysis."""
        symbol = message.symbol
        ts_ms = message.timestamp
        price = message.price

        # Get minute bucket
        minute_bucket = ts_ms // 60000

        # Update counters
        self.ticks_per_minute[symbol][minute_bucket] += 1
        self.ticks_per_symbol[symbol] += 1

        # Track price changes
        if symbol in self.last_tick_time:
            last_price = self.price_changes[symbol][-1] if self.price_changes[symbol] else price
            if price != last_price:
                self.price_changes[symbol].append(price)
        else:
            self.price_changes[symbol].append(price)

        self.last_tick_time[symbol] = ts_ms

    def get_stats(self) -> Dict:
        """Get current statistics."""
        elapsed = time.time() - self.start_time

        stats = {
            'elapsed_seconds': elapsed,
            'symbols': {},
        }

        for symbol in self.ticks_per_symbol:
            total_ticks = self.ticks_per_symbol[symbol]
            minute_buckets = self.ticks_per_minute[symbol]

            ticks_per_min = list(minute_buckets.values())
            unique_prices = len(self.price_changes[symbol])

            stats['symbols'][symbol] = {
                'total_ticks': total_ticks,
                'ticks_per_second': total_ticks / elapsed if elapsed > 0 else 0,
                'avg_ticks_per_minute': sum(ticks_per_min) / len(ticks_per_min) if ticks_per_min else 0,
                'min_ticks_per_minute': min(ticks_per_min) if ticks_per_min else 0,
                'max_ticks_per_minute': max(ticks_per_min) if ticks_per_min else 0,
                'unique_prices': unique_prices,
                'last_tick_time': self.last_tick_time.get(symbol),
            }

        return stats

    def print_stats(self):
        """Print current statistics."""
        stats = self.get_stats()
        elapsed = stats['elapsed_seconds']

        print(f"\n{'='*70}")
        print(f"📊 TICK MONITOR STATS (Elapsed: {elapsed:.1f}s)")
        print(f"{'='*70}")

        for symbol, symbol_stats in stats['symbols'].items():
            print(f"\n📈 {symbol}:")
            print(f"   Total ticks: {symbol_stats['total_ticks']}")
            print(f"   Ticks/second: {symbol_stats['ticks_per_second']:.2f}")
            print(f"   Avg ticks/minute: {symbol_stats['avg_ticks_per_minute']:.1f}")
            print(f"   Min ticks/minute: {symbol_stats['min_ticks_per_minute']}")
            print(f"   Max ticks/minute: {symbol_stats['max_ticks_per_minute']}")
            print(f"   Unique prices seen: {symbol_stats['unique_prices']}")

            if symbol_stats['last_tick_time']:
                last_dt = datetime.fromtimestamp(
                    symbol_stats['last_tick_time'] / 1000,
                    tz=timezone.utc
                ).astimezone(self.ist)
                print(f"   Last tick: {last_dt.strftime('%H:%M:%S')}")


async def monitor_redis_tick_rate(symbols: List[str], duration_seconds: int = 300):
    """
    Monitor tick rate by watching Redis TimeSeries growth.
    This doesn't require modifying the ingestion pipeline.
    """
    print(f"\n{'='*70}")
    print(f"🔴 REDIS TICK RATE MONITOR")
    print(f"Duration: {duration_seconds} seconds")
    print(f"Symbols: {', '.join(symbols)}")
    print(f"{'='*70}")

    redis = get_redis()
    if not redis:
        print("❌ Redis not available!")
        return

    ist = pytz.timezone('Asia/Kolkata')

    # Track sample counts
    previous_counts: Dict[str, int] = {}
    tick_rates: Dict[str, List[int]] = defaultdict(list)

    # Initialize counts
    for symbol in symbols:
        price_key = f"{symbol}:price"
        try:
            info = await redis.ts().info(price_key)
            previous_counts[symbol] = info.total_samples
        except:
            previous_counts[symbol] = 0
            print(f"⚠️  No existing data for {symbol}")

    print(f"\nStarted at: {datetime.now(ist).strftime('%H:%M:%S')}")
    print("Monitoring tick rate every 10 seconds...\n")

    interval = 10  # Check every 10 seconds
    iterations = duration_seconds // interval

    for i in range(iterations):
        await asyncio.sleep(interval)

        now = datetime.now(ist)
        print(f"\n⏱️  {now.strftime('%H:%M:%S')}")

        for symbol in symbols:
            price_key = f"{symbol}:price"
            try:
                info = await redis.ts().info(price_key)
                current_count = info.total_samples
                new_ticks = current_count - previous_counts.get(symbol, 0)
                previous_counts[symbol] = current_count
                tick_rates[symbol].append(new_ticks)

                ticks_per_sec = new_ticks / interval
                status = "✅" if new_ticks > 10 else "⚠️" if new_ticks > 0 else "❌"

                print(f"   {status} {symbol}: {new_ticks} new ticks ({ticks_per_sec:.2f}/sec)")

            except Exception as e:
                print(f"   ❌ {symbol}: Error - {e}")
                tick_rates[symbol].append(0)

    # Summary
    print(f"\n{'='*70}")
    print(f"📊 FINAL SUMMARY")
    print(f"{'='*70}")

    for symbol in symbols:
        rates = tick_rates[symbol]
        if rates:
            print(f"\n{symbol}:")
            print(f"   Total ticks: {sum(rates)}")
            print(f"   Avg per {interval}s: {sum(rates)/len(rates):.1f}")
            print(f"   Min per {interval}s: {min(rates)}")
            print(f"   Max per {interval}s: {max(rates)}")

            zero_periods = rates.count(0)
            if zero_periods > 0:
                print(f"   ⚠️  Zero-tick periods: {zero_periods} ({zero_periods*100/len(rates):.1f}%)")

            # Calculate expected vs actual
            expected_per_10s = 50  # Rough expectation for liquid stocks
            actual_avg = sum(rates)/len(rates)
            if actual_avg < expected_per_10s * 0.1:
                print(f"   🚨 CRITICAL: Tick rate is {actual_avg/expected_per_10s*100:.1f}% of expected!")
                print(f"      This suggests major data delivery issues from Dhan")


async def analyze_timestamp_distribution(symbol: str):
    """
    Analyze timestamp distribution to detect timing issues.
    Checks if ticks are evenly distributed or clustered.
    """
    print(f"\n{'='*70}")
    print(f"📊 TIMESTAMP DISTRIBUTION ANALYSIS: {symbol}")
    print(f"{'='*70}")

    redis = get_redis()
    if not redis:
        print("❌ Redis not available!")
        return

    price_key = f"{symbol}:price"
    ist = pytz.timezone('Asia/Kolkata')

    try:
        # Get all available samples
        samples = await redis.ts().range(
            price_key,
            from_time="-",
            to_time="+",
        )

        if not samples:
            print("❌ No samples available")
            return

        print(f"\n📈 Found {len(samples)} samples")

        # Analyze gaps between consecutive ticks
        gaps = []
        for i in range(1, len(samples)):
            gap_ms = samples[i][0] - samples[i-1][0]
            gaps.append(gap_ms)

        if gaps:
            print(f"\n📊 Gap Analysis (time between consecutive ticks):")
            print(f"   Min gap: {min(gaps)}ms")
            print(f"   Max gap: {max(gaps)}ms ({max(gaps)/1000:.1f}s)")
            print(f"   Avg gap: {sum(gaps)/len(gaps):.1f}ms")

            # Count large gaps (> 30 seconds)
            large_gaps = [g for g in gaps if g > 30000]
            if large_gaps:
                print(f"\n⚠️  Large gaps (> 30s): {len(large_gaps)}")
                print(f"   Largest gaps:")
                sorted_gaps = sorted(zip(gaps, range(len(gaps))), reverse=True)[:5]
                for gap_ms, idx in sorted_gaps:
                    gap_start = samples[idx][0]
                    dt = datetime.fromtimestamp(gap_start/1000, tz=timezone.utc).astimezone(ist)
                    print(f"      {dt.strftime('%H:%M:%S')} -> gap of {gap_ms/1000:.1f}s")

            # Count very small gaps (< 100ms) - rapid ticks
            rapid_ticks = len([g for g in gaps if g < 100])
            print(f"\n📈 Rapid ticks (< 100ms apart): {rapid_ticks} ({rapid_ticks*100/len(gaps):.1f}%)")

        # Analyze distribution within each minute
        print(f"\n📊 Ticks per second distribution:")
        second_counts = defaultdict(int)
        for ts, _ in samples:
            second_bucket = ts // 1000
            second_counts[second_bucket] += 1

        counts = list(second_counts.values())
        if counts:
            print(f"   Min per second: {min(counts)}")
            print(f"   Max per second: {max(counts)}")
            print(f"   Avg per second: {sum(counts)/len(counts):.2f}")

            # Count seconds with 0 or 1 tick
            sparse_seconds = len([c for c in counts if c <= 1])
            print(f"\n   Seconds with ≤1 tick: {sparse_seconds} ({sparse_seconds*100/len(counts):.1f}%)")

    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()


async def check_connection_stability():
    """
    Check WebSocket connection stability by looking at data patterns.
    Gaps in data might indicate connection drops.
    """
    print(f"\n{'='*70}")
    print(f"📊 CONNECTION STABILITY CHECK")
    print(f"{'='*70}")

    redis = get_redis()
    if not redis:
        print("❌ Redis not available!")
        return

    # Get all keys
    try:
        price_keys = await redis.ts().queryindex(["ts=price"])
        print(f"\n📈 Found {len(price_keys)} active price streams")

        # Check each symbol for gaps
        ist = pytz.timezone('Asia/Kolkata')
        symbols_with_gaps = []

        for key in price_keys[:10]:  # Check first 10
            key_str = key.decode('utf-8') if isinstance(key, bytes) else str(key)
            symbol = key_str.replace(':price', '')

            samples = await redis.ts().range(key_str, from_time="-", to_time="+")

            if len(samples) > 1:
                gaps = []
                for i in range(1, len(samples)):
                    gap_ms = samples[i][0] - samples[i-1][0]
                    if gap_ms > 60000:  # Gaps > 1 minute
                        gaps.append((samples[i-1][0], gap_ms))

                if gaps:
                    symbols_with_gaps.append((symbol, gaps))

        if symbols_with_gaps:
            print(f"\n⚠️  Symbols with large gaps (> 1 minute):")
            for symbol, gaps in symbols_with_gaps:
                print(f"\n   {symbol}: {len(gaps)} gaps")
                for gap_start, gap_ms in gaps[:3]:
                    dt = datetime.fromtimestamp(gap_start/1000, tz=timezone.utc).astimezone(ist)
                    print(f"      {dt.strftime('%H:%M:%S')} -> {gap_ms/1000:.1f}s gap")
        else:
            print(f"\n✅ No significant gaps detected in checked symbols")

    except Exception as e:
        print(f"❌ Error: {e}")


async def main():
    """Main entry point."""
    import argparse

    parser = argparse.ArgumentParser(description="Real-Time Tick Monitor")
    parser.add_argument("--symbols", nargs="+", default=["KOTAKBANK"], help="Symbols to monitor")
    parser.add_argument("--duration", type=int, default=300, help="Duration in seconds")
    parser.add_argument("--mode", choices=["rate", "timestamps", "stability", "all"], default="all")

    args = parser.parse_args()

    print("=" * 70)
    print("🔍 REAL-TIME TICK MONITOR")
    print("=" * 70)
    print(f"Time: {datetime.now()}")
    print(f"Symbols: {', '.join(args.symbols)}")
    print("=" * 70)

    if args.mode in ["rate", "all"]:
        await monitor_redis_tick_rate(args.symbols, args.duration)

    if args.mode in ["timestamps", "all"]:
        for symbol in args.symbols:
            await analyze_timestamp_distribution(symbol)

    if args.mode in ["stability", "all"]:
        await check_connection_stability()


if __name__ == "__main__":
    asyncio.run(main())

