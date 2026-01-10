"""
Gap Detection Service

Detects and fills gaps in Redis TimeSeries data on startup or reconnect.
Gaps are filled by fetching historical data from the provider (e.g., Dhan API)
and inserting directly into Redis 5m keys.
"""

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import List, Optional, Dict, Set
import pytz
from datetime import date as DateType

from sqlalchemy import select

from config.database_config import get_db_session
from config.logger import logger
from models import Instrument, Exchange
from services.redis_timeseries import get_redis_timeseries, RedisTimeSeries


@dataclass
class GapInfo:
    """Represents a gap in data for a symbol."""
    symbol: str
    instrument_id: int
    exchange_id: int
    start_ts: int  # Start of gap (ms)
    end_ts: int    # End of gap (ms)


class GapDetectionService:
    """
    Service to detect and fill gaps in Redis TimeSeries data.

    Use cases:
    1. Server restart: Check if market was open while server was down
    2. WebSocket reconnect: Fill gap between disconnect and reconnect
    """

    def __init__(self, provider_manager=None):
        self.redis_ts: RedisTimeSeries = get_redis_timeseries()
        self.provider_manager = provider_manager
        self._recordable_instruments: Dict[str, Instrument] = {}  # symbol -> Instrument
        self._exchange_cache: Dict[int, Exchange] = {}  # exchange_id -> Exchange

    async def initialize(self):
        """Load recordable instruments from database."""
        logger.info("Initializing GapDetectionService...")

        async for session in get_db_session():
            # Load instruments with should_record_data=True
            stmt = (
                select(Instrument)
                .where(Instrument.should_record_data == True)
                .options()  # Add any needed eager loads
            )
            result = await session.execute(stmt)
            instruments = result.scalars().all()

            for inst in instruments:
                self._recordable_instruments[inst.symbol] = inst

                # Cache exchange
                if inst.exchange_id not in self._exchange_cache:
                    exchange = await session.get(Exchange, inst.exchange_id)
                    if exchange:
                        self._exchange_cache[inst.exchange_id] = exchange

            logger.info(f"Loaded {len(self._recordable_instruments)} recordable instruments")
            break

    def _get_exchange_trading_hours(self, exchange: Exchange, date: DateType) -> tuple[int, int]:
        """
        Get trading hours for an exchange on a specific date.
        Returns (market_open_ts_ms, market_close_ts_ms).
        """
        tz = pytz.timezone(exchange.timezone or "UTC")

        if exchange.market_open_time and exchange.market_close_time:
            open_dt = tz.localize(datetime.combine(date, exchange.market_open_time))
            close_dt = tz.localize(datetime.combine(date, exchange.market_close_time))
            return int(open_dt.timestamp() * 1000), int(close_dt.timestamp() * 1000)

        # Fallback: assume 9:00 - 16:00 local time
        open_dt = tz.localize(datetime.combine(date, datetime.strptime("09:00", "%H:%M").time()))
        close_dt = tz.localize(datetime.combine(date, datetime.strptime("16:00", "%H:%M").time()))
        return int(open_dt.timestamp() * 1000), int(close_dt.timestamp() * 1000)

    def _is_within_trading_hours(self, ts_ms: int, exchange: Exchange) -> bool:
        """Check if a timestamp falls within trading hours."""
        dt = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
        tz = pytz.timezone(exchange.timezone or "UTC")
        local_dt = dt.astimezone(tz)

        market_open, market_close = self._get_exchange_trading_hours(exchange, local_dt.date())
        return market_open <= ts_ms <= market_close

    async def detect_gaps_on_startup(self) -> List[GapInfo]:
        """
        Detect gaps in data since last known timestamp for each recordable instrument.
        Called on server startup.

        Returns list of gaps that need to be filled.
        """
        logger.info("🔍 Detecting gaps on startup...")
        gaps: List[GapInfo] = []
        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)

        for symbol, instrument in self._recordable_instruments.items():
            try:
                exchange = self._exchange_cache.get(instrument.exchange_id)
                if not exchange:
                    continue

                # Get latest 5m candle timestamp for this symbol
                last_5m_ts = await self.redis_ts.get_latest_5m_timestamp(symbol)

                if last_5m_ts is None:
                    # No data at all - check if market is open today
                    tz = pytz.timezone(exchange.timezone or "UTC")
                    today = datetime.now(tz).date()
                    market_open, market_close = self._get_exchange_trading_hours(exchange, today)

                    if market_open <= now_ms <= market_close:
                        # Market is open, we have a gap from market open until now
                        gaps.append(GapInfo(
                            symbol=symbol,
                            instrument_id=instrument.id,
                            exchange_id=instrument.exchange_id,
                            start_ts=market_open,
                            end_ts=now_ms,
                        ))
                        logger.debug(f"Gap detected for {symbol}: No data, market is open")
                    continue

                # We have some data - check if there's a gap
                expected_next_ts = last_5m_ts + (5 * 60 * 1000)  # 5 minutes later

                if now_ms > expected_next_ts:
                    # Potential gap - but only if within trading hours
                    if self._is_within_trading_hours(now_ms, exchange):
                        gaps.append(GapInfo(
                            symbol=symbol,
                            instrument_id=instrument.id,
                            exchange_id=instrument.exchange_id,
                            start_ts=expected_next_ts,
                            end_ts=now_ms,
                        ))
                        gap_minutes = (now_ms - expected_next_ts) / (60 * 1000)
                        logger.debug(f"Gap detected for {symbol}: {gap_minutes:.1f} minutes")

            except Exception as e:
                logger.error(f"Error detecting gap for {symbol}: {e}")

        logger.info(f"🔍 Found {len(gaps)} gaps to fill")
        return gaps

    async def detect_gaps_on_reconnect(
        self,
        disconnect_ts: int,
        reconnect_ts: int,
        symbols: Optional[Set[str]] = None
    ) -> List[GapInfo]:
        """
        Detect gaps caused by a WebSocket disconnect.

        Args:
            disconnect_ts: Timestamp when connection was lost (ms)
            reconnect_ts: Timestamp when connection was restored (ms)
            symbols: Optional set of symbols to check (defaults to all recordable)

        Returns list of gaps that need to be filled.
        """
        logger.info(f"🔍 Detecting gaps from reconnect ({(reconnect_ts - disconnect_ts) / 1000:.1f}s disconnect)...")
        gaps: List[GapInfo] = []

        check_symbols = symbols if symbols else set(self._recordable_instruments.keys())

        for symbol in check_symbols:
            instrument = self._recordable_instruments.get(symbol)
            if not instrument:
                continue

            exchange = self._exchange_cache.get(instrument.exchange_id)
            if not exchange:
                continue

            # Check if the gap period overlaps with trading hours
            if not self._is_within_trading_hours(disconnect_ts, exchange):
                continue

            # Align to 5-minute boundaries
            gap_start = self._align_to_5m_boundary(disconnect_ts, ceiling=True)
            gap_end = self._align_to_5m_boundary(reconnect_ts, ceiling=False)

            if gap_end > gap_start:
                gaps.append(GapInfo(
                    symbol=symbol,
                    instrument_id=instrument.id,
                    exchange_id=instrument.exchange_id,
                    start_ts=gap_start,
                    end_ts=gap_end,
                ))

        logger.info(f"🔍 Found {len(gaps)} gaps from reconnect")
        return gaps

    def _align_to_5m_boundary(self, ts_ms: int, ceiling: bool = False) -> int:
        """Align timestamp to 5-minute boundary."""
        interval_ms = 5 * 60 * 1000
        if ceiling:
            remainder = ts_ms % interval_ms
            if remainder == 0:
                return ts_ms
            return ts_ms + (interval_ms - remainder)
        else:
            return (ts_ms // interval_ms) * interval_ms

    async def fill_gaps(self, gaps: List[GapInfo]) -> Dict[str, int]:
        """
        Fill gaps by fetching data from provider and inserting into Redis.

        Returns dict of symbol -> number of candles inserted.
        """
        if not gaps:
            return {}

        if not self.provider_manager:
            logger.error("Cannot fill gaps: provider_manager not set")
            return {}

        logger.info(f"🔄 Filling {len(gaps)} gaps...")
        results: Dict[str, int] = {}

        # Group gaps by exchange for efficient API calls
        gaps_by_exchange: Dict[int, List[GapInfo]] = {}
        for gap in gaps:
            if gap.exchange_id not in gaps_by_exchange:
                gaps_by_exchange[gap.exchange_id] = []
            gaps_by_exchange[gap.exchange_id].append(gap)

        for exchange_id, exchange_gaps in gaps_by_exchange.items():
            try:
                await self._fill_gaps_for_exchange(exchange_id, exchange_gaps, results)
            except Exception as e:
                logger.error(f"Error filling gaps for exchange {exchange_id}: {e}")

        total_candles = sum(results.values())
        logger.info(f"✅ Filled {total_candles} candles across {len(results)} symbols")
        return results

    async def _fill_gaps_for_exchange(
        self,
        exchange_id: int,
        gaps: List[GapInfo],
        results: Dict[str, int]
    ):
        """Fill gaps for a specific exchange."""
        # Get instruments for this batch
        instruments: List[Instrument] = []
        gap_map: Dict[str, GapInfo] = {}

        for gap in gaps:
            instrument = self._recordable_instruments.get(gap.symbol)
            if instrument:
                instruments.append(instrument)
                gap_map[gap.symbol] = gap

        if not instruments:
            return

        # Find the overall time range for this batch
        min_start = min(g.start_ts for g in gaps)
        max_end = max(g.end_ts for g in gaps)

        start_dt = datetime.fromtimestamp(min_start / 1000, tz=timezone.utc)
        end_dt = datetime.fromtimestamp(max_end / 1000, tz=timezone.utc)

        logger.info(f"Fetching historical data for {len(instruments)} instruments from {start_dt} to {end_dt}")

        # Get correct provider for this exchange from ProviderManager maps
        provider_code = self.provider_manager.exchange_to_provider.get(exchange_id)
        provider = self.provider_manager.providers.get(provider_code)

        if not provider:
            logger.error(f"No provider found for exchange {exchange_id} (Code: {provider_code})")
            return

        try:
            # Fetch intraday prices from provider
            historical_data = await provider.get_intraday_prices(
                instruments=instruments,
                start_date=start_dt,
                end_date=end_dt,
                timeframe='5m'
            )

            # Insert fetched data into Redis 5m keys
            for symbol, candles in historical_data.items():
                gap = gap_map.get(symbol)
                if not gap:
                    continue

                count = 0
                for candle in candles:
                    candle_ts = int(candle.datetime.timestamp() * 1000)

                    # Only insert candles within the gap period
                    if gap.start_ts <= candle_ts <= gap.end_ts:
                        await self.redis_ts.add_5m_candle(
                            symbol=symbol,
                            timestamp_ms=candle_ts,
                            open_price=candle.open,
                            high_price=candle.high,
                            low_price=candle.low,
                            close_price=candle.close,
                            volume=candle.volume or 0,
                        )
                        count += 1

                if count > 0:
                    results[symbol] = count
                    logger.debug(f"Filled {count} candles for {symbol}")

        except Exception as e:
            logger.error(f"Error fetching historical data: {e}")

    async def fill_gaps_from_startup(self):
        """
        Convenience method to detect and fill gaps on startup.
        Call this after initialize().
        """
        gaps = await self.detect_gaps_on_startup()
        if gaps:
            await self.fill_gaps(gaps)


# Singleton
_gap_detection_service: Optional[GapDetectionService] = None


def get_gap_detection_service() -> GapDetectionService:
    global _gap_detection_service
    if _gap_detection_service is None:
        _gap_detection_service = GapDetectionService()
    return _gap_detection_service


def set_gap_detection_provider(provider_manager):
    """Set the provider manager for gap detection service."""
    service = get_gap_detection_service()
    service.provider_manager = provider_manager
