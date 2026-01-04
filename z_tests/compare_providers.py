"""
Test script to compare DhanProvider (manual) vs DhanHQProvider (dhanhq library)

This script will:
1. Initialize both providers
2. Subscribe to the same symbols
3. Monitor tick rates from each
4. Compare data quality

Run during market hours for meaningful comparison.
"""

import asyncio
import sys
import time
from datetime import datetime, timezone
from collections import defaultdict
from typing import Dict
import threading

sys.path.insert(0, r"C:\Users\tech\OneDrive\Documents\GitHub\RealtimeMarketApplication")

import pytz
from config.settings import get_settings
from config.logger import logger
from utils.common_constants import DataIngestionFormat


class TickCollector:
    """Collects ticks for analysis."""

    def __init__(self, name: str):
        self.name = name
        self.tick_count = 0
        self.ticks_by_symbol: Dict[str, list] = defaultdict(list)
        self.start_time = None
        self.last_tick_time = None

    def record_tick(self, message: DataIngestionFormat):
        if self.start_time is None:
            self.start_time = time.time()

        self.tick_count += 1
        self.last_tick_time = time.time()

        self.ticks_by_symbol[message.symbol].append({
            'timestamp': message.timestamp,
            'price': message.price,
            'volume': message.volume,
            'received_at': time.time(),
        })

    def get_stats(self) -> dict:
        elapsed = time.time() - self.start_time if self.start_time else 0
        return {
            'name': self.name,
            'total_ticks': self.tick_count,
            'ticks_per_second': self.tick_count / elapsed if elapsed > 0 else 0,
            'unique_symbols': len(self.ticks_by_symbol),
            'elapsed_seconds': elapsed,
        }


async def test_dhan_hq_provider_only(duration_seconds: int = 60):
    """Test only the DhanHQ provider."""
    print("=" * 70)
    print("[TEST] TESTING DhanHQ PROVIDER (dhanhq library)")
    print("=" * 70)

    from services.provider.dhan_hq_provider import DhanHQProvider

    # Create collector
    hq_collector = TickCollector("DhanHQ")

    # Create provider
    hq_provider = DhanHQProvider(
        callback=hq_collector.record_tick,
        provider_manager=None
    )

    # Test symbols (Dhan Security IDs)
    # KOTAKBANK = 1922, RELIANCE = 2885, INFY = 1594
    test_symbols = ["NSE_EQ:1922", "NSE_EQ:2885", "NSE_EQ:1594"]

    print(f"\n[CONNECT] Connecting to {len(test_symbols)} symbols...")
    print(f"   Symbols: {test_symbols}")

    try:
        # Connect
        hq_provider.connect_websocket(test_symbols)

        print(f"\n[WAIT] Collecting data for {duration_seconds} seconds...")

        # Monitor progress
        for i in range(duration_seconds // 10):
            await asyncio.sleep(10)
            stats = hq_collector.get_stats()
            print(f"   [{i*10+10}s] Ticks: {stats['total_ticks']}, Rate: {stats['ticks_per_second']:.2f}/s")

        # Final stats
        await asyncio.sleep(duration_seconds % 10)

    except Exception as e:
        print(f"[ERROR] Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        # Disconnect
        print("\n[DISCONNECT] Disconnecting...")
        hq_provider.disconnect_websocket()

    # Print results
    stats = hq_collector.get_stats()
    print(f"\n{'='*70}")
    print(f"[RESULTS] RESULTS: {stats['name']}")
    print(f"{'='*70}")
    print(f"   Total ticks received: {stats['total_ticks']}")
    print(f"   Ticks per second: {stats['ticks_per_second']:.2f}")
    print(f"   Unique symbols: {stats['unique_symbols']}")
    print(f"   Duration: {stats['elapsed_seconds']:.1f}s")

    # Ticks by symbol
    print(f"\n[TICKS] Ticks by Symbol:")
    for symbol, ticks in hq_collector.ticks_by_symbol.items():
        unique_prices = len(set(t['price'] for t in ticks))
        print(f"   {symbol}: {len(ticks)} ticks, {unique_prices} unique prices")

    return stats


async def compare_providers(duration_seconds: int = 120):
    """Compare both providers side by side."""
    print("=" * 70)
    print("🧪 COMPARING PROVIDERS: Manual vs dhanhq Library")
    print("=" * 70)
    print(f"Duration: {duration_seconds} seconds")
    print("=" * 70)

    from services.provider.dhan_provider import DhanProvider
    from services.provider.dhan_hq_provider import DhanHQProvider

    # Create collectors
    manual_collector = TickCollector("Manual Implementation")
    hq_collector = TickCollector("dhanhq Library")

    # Create providers
    manual_provider = DhanProvider(
        callback=manual_collector.record_tick,
        provider_manager=None
    )

    hq_provider = DhanHQProvider(
        callback=hq_collector.record_tick,
        provider_manager=None
    )

    # Test symbols
    test_symbols = ["NSE_EQ:1922", "NSE_EQ:2885", "NSE_EQ:1594"]

    print(f"\n📡 Connecting both providers to: {test_symbols}")

    try:
        # Connect both
        await manual_provider.connect_websocket(test_symbols)
        hq_provider.connect_websocket(test_symbols)

        print(f"\n⏳ Collecting data for {duration_seconds} seconds...")

        # Monitor progress
        for i in range(duration_seconds // 10):
            await asyncio.sleep(10)
            manual_stats = manual_collector.get_stats()
            hq_stats = hq_collector.get_stats()
            print(f"   [{i*10+10}s] Manual: {manual_stats['total_ticks']} ticks | HQ: {hq_stats['total_ticks']} ticks")

    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        # Disconnect both
        print("\n📡 Disconnecting both providers...")
        manual_provider.disconnect_websocket()
        hq_provider.disconnect_websocket()

    # Compare results
    manual_stats = manual_collector.get_stats()
    hq_stats = hq_collector.get_stats()

    print(f"\n{'='*70}")
    print(f"📊 COMPARISON RESULTS")
    print(f"{'='*70}")
    print(f"{'Metric':<30} | {'Manual':<15} | {'dhanhq':<15}")
    print(f"{'-'*30}-+-{'-'*15}-+-{'-'*15}")
    print(f"{'Total Ticks':<30} | {manual_stats['total_ticks']:<15} | {hq_stats['total_ticks']:<15}")
    print(f"{'Ticks/Second':<30} | {manual_stats['ticks_per_second']:<15.2f} | {hq_stats['ticks_per_second']:<15.2f}")
    print(f"{'Unique Symbols':<30} | {manual_stats['unique_symbols']:<15} | {hq_stats['unique_symbols']:<15}")

    # Determine winner
    if hq_stats['total_ticks'] > manual_stats['total_ticks'] * 1.1:
        print(f"\n🏆 Winner: dhanhq Library ({hq_stats['total_ticks']/manual_stats['total_ticks']*100-100:.1f}% more ticks)")
    elif manual_stats['total_ticks'] > hq_stats['total_ticks'] * 1.1:
        print(f"\n🏆 Winner: Manual Implementation ({manual_stats['total_ticks']/hq_stats['total_ticks']*100-100:.1f}% more ticks)")
    else:
        print(f"\n🤝 Both providers are comparable")

    return manual_stats, hq_stats


async def main():
    """Main entry point."""
    import argparse

    parser = argparse.ArgumentParser(description="Compare Dhan Provider Implementations")
    parser.add_argument("--mode", choices=["hq", "compare"], default="hq",
                        help="Test mode: 'hq' for DhanHQ only, 'compare' for both")
    parser.add_argument("--duration", type=int, default=60, help="Test duration in seconds")

    args = parser.parse_args()

    ist = pytz.timezone('Asia/Kolkata')
    now = datetime.now(ist)

    print(f"\n[TIME] Current Time: {now.strftime('%Y-%m-%d %H:%M:%S')} IST")

    # Check if market is open
    market_open = now.replace(hour=9, minute=15, second=0)
    market_close = now.replace(hour=15, minute=30, second=0)

    if not (market_open <= now <= market_close):
        print("[WARNING] WARNING: Market is closed. You may not receive any ticks.")
        print("   This test should be run during market hours (9:15 AM - 3:30 PM IST)")

    if args.mode == "hq":
        await test_dhan_hq_provider_only(args.duration)
    else:
        await compare_providers(args.duration)


if __name__ == "__main__":
    asyncio.run(main())

