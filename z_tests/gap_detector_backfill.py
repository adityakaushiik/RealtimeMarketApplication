"""
SOLUTION: Dhan Data Gap Detector and Backfill Service

This service monitors for data gaps and automatically backfills missing
OHLC data using Dhan's REST API when gaps are detected.

Key Features:
1. Detects gaps in real-time data stream
2. Marks affected DB records for resolution
3. Uses Dhan REST API to fetch historical candles
4. Backfills missing data automatically

Author: Investigation & Solution Script
Date: 2025-12-23
"""

import asyncio
import sys
import time
from datetime import datetime, timedelta, timezone, date
from collections import defaultdict
from typing import Dict, List, Optional, Tuple
import pytz

sys.path.insert(0, r"C:\Users\tech\OneDrive\Documents\GitHub\RealtimeMarketApplication")

from sqlalchemy import select, update
from sqlalchemy.orm import joinedload

from config.database_config import get_db_session
from config.redis_config import get_redis
from config.logger import logger
from config.settings import get_settings
from models import Instrument, PriceHistoryIntraday, ProviderInstrumentMapping, Provider

import requests


class DhanGapDetectorAndBackfill:
    """
    Detects gaps in market data and backfills using Dhan REST API.
    """

    # Dhan REST API endpoints
    REST_URL = "https://api.dhan.co/v2"

    def __init__(self):
        settings = get_settings()
        self.access_token = settings.DHAN_ACCESS_TOKEN
        self.client_id = settings.DHAN_CLIENT_ID
        self.ist = pytz.timezone('Asia/Kolkata')
        self.redis = None

        # Gap detection thresholds
        self.min_expected_ticks_per_5min = 10  # Minimum expected for liquid stocks
        self.gap_threshold_seconds = 120  # 2 minutes without data = gap

    async def initialize(self):
        """Initialize connections."""
        self.redis = get_redis()
        logger.info("DhanGapDetectorAndBackfill initialized")

    def _make_api_request(self, endpoint: str, payload: dict) -> dict:
        """Make REST API request to Dhan."""
        url = f"{self.REST_URL}{endpoint}"
        headers = {
            "Content-Type": "application/json",
            "access-token": self.access_token,
            "Accept": "application/json",
        }

        try:
            response = requests.post(url, json=payload, headers=headers)

            if response.status_code == 429:
                logger.warning("Rate limit hit, waiting...")
                time.sleep(1)
                return self._make_api_request(endpoint, payload)

            if not response.ok:
                logger.error(f"Dhan API error: {response.status_code} - {response.text}")
                return {}

            return response.json()
        except Exception as e:
            logger.error(f"Dhan API request failed: {e}")
            return {}

    def fetch_intraday_candles(
        self,
        security_id: str,
        exchange_segment: str = "NSE_EQ",
        from_date: date = None,
        to_date: date = None
    ) -> List[Dict]:
        """
        Fetch intraday candles from Dhan REST API.

        Returns list of candles:
        [{"timestamp": ..., "open": ..., "high": ..., "low": ..., "close": ..., "volume": ...}, ...]
        """
        from_date = from_date or date.today()
        to_date = to_date or date.today()

        # Format dates as required by Dhan API
        from_str = from_date.strftime("%Y-%m-%d")
        to_str = to_date.strftime("%Y-%m-%d")

        payload = {
            "securityId": security_id,
            "exchangeSegment": exchange_segment,
            "instrument": "EQUITY",
            "interval": "5",  # 5-minute candles
            "fromDate": from_str,
            "toDate": to_str,
        }

        logger.info(f"Fetching intraday candles for {security_id} from {from_str} to {to_str}")

        response = self._make_api_request("/charts/intraday", payload)

        if not response:
            return []

        # Parse response - Dhan returns data in OHLCV format
        # Structure: {"open": [...], "high": [...], "low": [...], "close": [...], "volume": [...], "timestamp": [...]}
        candles = []

        if "open" in response and "timestamp" in response:
            opens = response.get("open", [])
            highs = response.get("high", [])
            lows = response.get("low", [])
            closes = response.get("close", [])
            volumes = response.get("volume", [])
            timestamps = response.get("timestamp", [])

            for i in range(len(timestamps)):
                candles.append({
                    "timestamp": timestamps[i],
                    "open": opens[i] if i < len(opens) else None,
                    "high": highs[i] if i < len(highs) else None,
                    "low": lows[i] if i < len(lows) else None,
                    "close": closes[i] if i < len(closes) else None,
                    "volume": volumes[i] if i < len(volumes) else 0,
                })

        logger.info(f"Fetched {len(candles)} candles from Dhan REST API")
        return candles

    async def detect_gaps_in_db(
        self,
        symbol: str,
        target_date: date = None
    ) -> List[Tuple[datetime, str]]:
        """
        Detect gaps (singular or null OHLC) in database records.

        Returns list of (datetime, issue_type) tuples.
        """
        target_date = target_date or date.today()
        gaps = []

        async for session in get_db_session():
            # Get instrument
            stmt = (
                select(Instrument)
                .options(joinedload(Instrument.exchange))
                .where(Instrument.symbol == symbol)
            )
            result = await session.execute(stmt)
            instrument = result.scalar_one_or_none()

            if not instrument:
                logger.warning(f"Instrument {symbol} not found")
                return []

            exchange = instrument.exchange

            # Calculate date range
            market_open = exchange.market_open_time
            market_close = exchange.market_close_time

            start_dt = self.ist.localize(datetime.combine(target_date, market_open))
            end_dt = self.ist.localize(datetime.combine(target_date, market_close))

            # Fetch records
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

            for r in records:
                if r.open is None:
                    gaps.append((r.datetime, "NULL"))
                elif r.open == r.high == r.low == r.close:
                    # Check volume - very low volume indicates gap
                    if (r.volume or 0) < self.min_expected_ticks_per_5min:
                        gaps.append((r.datetime, "SINGULAR_LOW_VOL"))

        logger.info(f"Detected {len(gaps)} gaps for {symbol} on {target_date}")
        return gaps

    async def backfill_symbol(
        self,
        symbol: str,
        target_date: date = None
    ) -> int:
        """
        Backfill missing/singular data for a symbol using Dhan REST API.

        Returns number of records updated.
        """
        target_date = target_date or date.today()
        updated_count = 0

        async for session in get_db_session():
            # Get instrument and Dhan security ID
            stmt = (
                select(Instrument, ProviderInstrumentMapping.provider_instrument_search_code)
                .options(joinedload(Instrument.exchange))
                .join(ProviderInstrumentMapping, Instrument.id == ProviderInstrumentMapping.instrument_id)
                .join(Provider, ProviderInstrumentMapping.provider_id == Provider.id)
                .where(
                    Instrument.symbol == symbol,
                    Provider.code == "DHAN"
                )
            )
            result = await session.execute(stmt)
            row = result.first()

            if not row:
                logger.warning(f"No Dhan mapping found for {symbol}")
                return 0

            instrument, security_id = row
            exchange = instrument.exchange

            # Fetch candles from Dhan REST API
            candles = self.fetch_intraday_candles(
                security_id=security_id,
                exchange_segment="NSE_EQ",
                from_date=target_date,
                to_date=target_date
            )

            if not candles:
                logger.warning(f"No candles returned from Dhan for {symbol}")
                return 0

            # Convert candles to updates
            for candle in candles:
                # Parse timestamp - Dhan REST API returns epoch seconds that represent
                # the candle start time. Based on investigation, these timestamps are
                # already in UTC epoch format (not IST), so no adjustment needed.
                ts = candle.get("timestamp")
                if not ts:
                    continue

                # Convert to UTC datetime - timestamps from REST API are already UTC
                candle_dt_utc = datetime.fromtimestamp(ts, tz=timezone.utc)

                # Update database record
                stmt_update = (
                    update(PriceHistoryIntraday)
                    .where(
                        PriceHistoryIntraday.instrument_id == instrument.id,
                        PriceHistoryIntraday.datetime == candle_dt_utc,
                    )
                    .values(
                        open=candle.get("open"),
                        high=candle.get("high"),
                        low=candle.get("low"),
                        close=candle.get("close"),
                        volume=candle.get("volume", 0),
                        resolve_required=False,
                    )
                )

                result = await session.execute(stmt_update)
                if result.rowcount > 0:
                    updated_count += 1
                    logger.debug(f"Updated {symbol} at {candle_dt_utc}")

            await session.commit()

        logger.info(f"Backfilled {updated_count} records for {symbol}")
        return updated_count

    async def run_backfill_for_today(self, symbols: List[str] = None):
        """
        Run backfill for all specified symbols for today.
        If no symbols specified, backfill all symbols marked for recording.
        """
        today = date.today()

        async for session in get_db_session():
            if symbols is None:
                # Get all symbols marked for recording
                stmt = select(Instrument.symbol).where(Instrument.should_record_data == True)
                result = await session.execute(stmt)
                symbols = result.scalars().all()

        logger.info(f"Running backfill for {len(symbols)} symbols for {today}")

        total_updated = 0
        for symbol in symbols:
            gaps = await self.detect_gaps_in_db(symbol, today)
            if gaps:
                logger.info(f"{symbol}: Found {len(gaps)} gaps, running backfill...")
                updated = await self.backfill_symbol(symbol, today)
                total_updated += updated
            else:
                logger.debug(f"{symbol}: No gaps detected")

        logger.info(f"Backfill complete. Total records updated: {total_updated}")
        return total_updated


class ConnectionMonitor:
    """
    Monitors WebSocket connection health and logs events.
    Can be integrated into the Dhan provider for real-time monitoring.
    """

    def __init__(self):
        self.last_tick_time: Dict[str, int] = {}
        self.connection_drops: List[Dict] = []
        self.ist = pytz.timezone('Asia/Kolkata')

    def record_tick(self, symbol: str, timestamp_ms: int):
        """Record a tick and detect gaps."""
        if symbol in self.last_tick_time:
            gap_ms = timestamp_ms - self.last_tick_time[symbol]
            if gap_ms > 120000:  # 2 minute gap
                self.connection_drops.append({
                    'symbol': symbol,
                    'gap_start': self.last_tick_time[symbol],
                    'gap_end': timestamp_ms,
                    'duration_s': gap_ms / 1000,
                    'detected_at': datetime.now(self.ist).isoformat(),
                })
                logger.warning(
                    f"⚠️ Gap detected for {symbol}: {gap_ms/1000:.1f}s gap "
                    f"between {datetime.fromtimestamp(self.last_tick_time[symbol]/1000, tz=timezone.utc).astimezone(self.ist).strftime('%H:%M:%S')} "
                    f"and {datetime.fromtimestamp(timestamp_ms/1000, tz=timezone.utc).astimezone(self.ist).strftime('%H:%M:%S')}"
                )

        self.last_tick_time[symbol] = timestamp_ms

    def get_gaps_summary(self) -> List[Dict]:
        """Get summary of all detected gaps."""
        return self.connection_drops.copy()


async def main():
    """Main entry point for testing the backfill service."""
    import argparse

    parser = argparse.ArgumentParser(description="Gap Detector and Backfill Service")
    parser.add_argument("--symbol", default="KOTAKBANK", help="Symbol to backfill (or 'all' for all symbols)")
    parser.add_argument("--all", action="store_true", help="Backfill all symbols marked for recording")

    args = parser.parse_args()

    print("=" * 70)
    print("🔧 DHAN GAP DETECTOR AND BACKFILL SERVICE")
    print("=" * 70)
    print(f"Date: {date.today()}")
    print("=" * 70)

    service = DhanGapDetectorAndBackfill()
    await service.initialize()

    if args.all:
        # Backfill all symbols
        print(f"\n📊 Running backfill for ALL symbols...")
        updated = await service.run_backfill_for_today()
        print(f"✅ Total updated: {updated} records")
    else:
        # Test with specific symbol
        symbol = args.symbol

        print(f"\n📊 Detecting gaps for {symbol}...")
        gaps = await service.detect_gaps_in_db(symbol)

        if gaps:
            print(f"\n🔴 Found {len(gaps)} gaps:")
            for dt, issue_type in gaps[:10]:
                local_time = dt.astimezone(pytz.timezone('Asia/Kolkata'))
                print(f"   {local_time.strftime('%H:%M')} -> {issue_type}")

            print(f"\n🔧 Running backfill...")
            updated = await service.backfill_symbol(symbol)
            print(f"✅ Updated {updated} records")
        else:
            print(f"✅ No gaps detected for {symbol}")


if __name__ == "__main__":
    asyncio.run(main())

