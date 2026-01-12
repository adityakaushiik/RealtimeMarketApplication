import asyncio
import time
from typing import Dict, List, Optional, Set
from config.redis_config import get_redis
from config.logger import logger
from redis.exceptions import ResponseError

from utils.common_constants import DataBroadcastFormat


class RedisTimeSeries:
    """
    Helper around RedisTimeSeries module to store tick data (price, volume) and query OHLCV.

    Key Structure:
      Raw Ticks (15 minute retention):
        <symbol>:tick:price  - raw price ticks
        <symbol>:tick:volume - raw volume ticks (cumulative, uses max policy)

      Downsampled 5m Candles (full trading day retention ~18 hours):
        <symbol>:5m:open   - first price in 5m bucket
        <symbol>:5m:high   - max price in 5m bucket
        <symbol>:5m:low    - min price in 5m bucket
        <symbol>:5m:close  - last price in 5m bucket
        <symbol>:5m:volume - sum of volume in 5m bucket (or max-min for cumulative)

    Downsampling is handled automatically via TS.CREATERULE for instruments
    with should_record_data=True.

    PRIMARY CANDLE READING METHODS:
      - get_5m_candles(symbol, from_ts, to_ts) - Get historical 5m candles from downsampled keys
      - get_current_5m_candle(symbol) - Get ongoing candle from tick data (real-time)
      - get_current_daily_candle(symbol, day_start_ts) - Aggregate today's candles into daily OHLCV
      - get_all_intraday_candles(symbol, from_ts, to_ts) - Get 5m candles + current ongoing candle
    """

    # Retention periods
    TICK_RETENTION_MS = 15 * 60 * 1000  # 15 minutes for raw ticks
    CANDLE_5M_RETENTION_MS = 18 * 60 * 60 * 1000  # 18 hours for 5m candles (covers full trading day)
    BUCKET_5M_MS = 5 * 60 * 1000  # 5 minute buckets

    def __init__(self, redis_client=None) -> None:
        self._last_cumulative_volume: Dict[str, int] = {}
        self._redis = redis_client
        self._downsampling_initialized: Set[str] = set()

    def _get_client(self):
        if self._redis is None:
            self._redis = get_redis()
        return self._redis

    def _align_to_5m_slot(self, timestamp_ms: int, ceiling: bool = False) -> int:
        """Align a timestamp to the nearest 5-minute slot boundary."""
        interval_ms = self.BUCKET_5M_MS
        if ceiling:
            remainder = timestamp_ms % interval_ms
            if remainder == 0:
                return timestamp_ms
            return timestamp_ms + (interval_ms - remainder)
        else:
            return (timestamp_ms // interval_ms) * interval_ms

    # ============================================================
    # KEY NAMING HELPERS
    # ============================================================

    def _tick_price_key(self, symbol: str) -> str:
        return f"{symbol}:tick:price"

    def _tick_volume_key(self, symbol: str) -> str:
        return f"{symbol}:tick:volume"

    def _5m_open_key(self, symbol: str) -> str:
        return f"{symbol}:5m:open"

    def _5m_high_key(self, symbol: str) -> str:
        return f"{symbol}:5m:high"

    def _5m_low_key(self, symbol: str) -> str:
        return f"{symbol}:5m:low"

    def _5m_close_key(self, symbol: str) -> str:
        return f"{symbol}:5m:close"

    def _5m_volume_key(self, symbol: str) -> str:
        return f"{symbol}:5m:volume"

    # ============================================================
    # TIMESERIES CREATION & DOWNSAMPLING RULES
    # ============================================================

    async def create_tick_timeseries(self, symbol: str) -> None:
        """Create raw tick time series for a symbol with 15 minute retention."""
        r = self._get_client()
        price_key = self._tick_price_key(symbol)
        vol_key = self._tick_volume_key(symbol)

        # Create price series
        try:
            await r.ts().create(
                price_key,
                retention_msecs=self.TICK_RETENTION_MS,
                labels={"type": "tick", "field": "price", "symbol": symbol},
                duplicate_policy="last",
            )
        except ResponseError as e:
            if "key already exists" not in str(e).lower():
                logger.warning(f"Error creating timeseries {price_key}: {e}")

        # Create volume series
        try:
            await r.ts().create(
                vol_key,
                retention_msecs=self.TICK_RETENTION_MS,
                labels={"type": "tick", "field": "volume", "symbol": symbol},
                duplicate_policy="max",
            )
        except ResponseError as e:
            if "key already exists" not in str(e).lower():
                logger.warning(f"Error creating timeseries {vol_key}: {e}")

    async def create_5m_timeseries(self, symbol: str) -> None:
        """Create 5-minute downsampled time series for a symbol with full-day retention."""
        r = self._get_client()

        keys_config = [
            (self._5m_open_key(symbol), "open"),
            (self._5m_high_key(symbol), "high"),
            (self._5m_low_key(symbol), "low"),
            (self._5m_close_key(symbol), "close"),
            (self._5m_volume_key(symbol), "volume"),
        ]

        for key, field in keys_config:
            dup_policy = "sum" if field == "volume" else "last"
            try:
                await r.ts().create(
                    key,
                    retention_msecs=self.CANDLE_5M_RETENTION_MS,
                    labels={"type": "5m", "field": field, "symbol": symbol},
                    duplicate_policy=dup_policy,
                )
            except ResponseError as e:
                # Ignore "key already exists" errors
                if "key already exists" not in str(e).lower():
                     logger.warning(f"Error creating 5m timeseries {key}: {e}")

    async def setup_downsampling_rules(self, symbol: str) -> None:
        """Create TS.CREATERULE to automatically downsample ticks into 5m candles."""
        if symbol in self._downsampling_initialized:
            return

        r = self._get_client()
        tick_price = self._tick_price_key(symbol)
        tick_volume = self._tick_volume_key(symbol)

        rules = [
            (tick_price, self._5m_open_key(symbol), "first"),
            (tick_price, self._5m_high_key(symbol), "max"),
            (tick_price, self._5m_low_key(symbol), "min"),
            (tick_price, self._5m_close_key(symbol), "last"),
            (tick_volume, self._5m_volume_key(symbol), "range"),
        ]

        for source_key, dest_key, agg_type in rules:
            try:
                await r.ts().createrule(
                    source_key,
                    dest_key,
                    aggregation_type=agg_type,
                    bucket_size_msec=self.BUCKET_5M_MS,
                )
            except ResponseError as e:
                err_msg = str(e).lower()
                if "rule already exists" not in err_msg and "destination key already has a src rule" not in err_msg:
                    logger.warning(f"Error creating rule {source_key} -> {dest_key}: {e}")

        self._downsampling_initialized.add(symbol)
        logger.debug(f"Downsampling rules created for {symbol}")

    async def initialize_recordable_instruments(self, symbols: List[str]) -> None:
        """Initialize tick and 5m time series with downsampling rules for all recordable instruments."""

        for symbol in symbols:
            try:
                await self.create_tick_timeseries(symbol)
                await self.create_5m_timeseries(symbol)
                await self.setup_downsampling_rules(symbol)
            except Exception as e:
                logger.error(f"Error initializing timeseries for {symbol}: {e}")

        logger.info(f"✅ Initialized Redis TimeSeries for {len(symbols)} instruments")

    # ============================================================
    # TICK DATA INGESTION
    # ============================================================

    async def add_tick(
        self, symbol: str, timestamp: int, price: float, volume: float = 0.0
    ) -> None:
        """Add a raw tick to the tick time series."""
        r = self._get_client()
        price_key = self._tick_price_key(symbol)
        vol_key = self._tick_volume_key(symbol)

        try:
            async with r.pipeline() as pipe:
                pipe.ts().add(price_key, timestamp, float(price), on_duplicate="last")
                pipe.ts().add(vol_key, timestamp, float(volume), on_duplicate="max")
                await pipe.execute()
        except ResponseError as e:
            err_str = str(e).lower()
            if "key does not exist" in err_str:
                await self.create_tick_timeseries(symbol)
                async with r.pipeline() as pipe:
                    pipe.ts().add(price_key, timestamp, float(price), on_duplicate="last")
                    pipe.ts().add(vol_key, timestamp, float(volume), on_duplicate="max")
                    await pipe.execute()
            elif "timestamp cannot be older" in err_str:
                pass
            else:
                raise

    async def add_to_timeseries(
        self, key: str, timestamp: float, price: float, volume: float = 0.0
    ) -> None:
        """Legacy method - redirects to add_tick."""
        await self.add_tick(key, int(timestamp), price, volume)

    # ============================================================
    # DIRECT 5M CANDLE INSERTION (for gap filling)
    # ============================================================

    async def add_5m_candle(
        self,
        symbol: str,
        timestamp_ms: int,
        open_price: float,
        high_price: float,
        low_price: float,
        close_price: float,
        volume: float,
    ) -> None:
        """Directly insert a 5m candle (used for gap filling from historical API)."""
        r = self._get_client()

        try:
            async with r.pipeline() as pipe:
                pipe.ts().add(self._5m_open_key(symbol), timestamp_ms, float(open_price), on_duplicate="first")
                pipe.ts().add(self._5m_high_key(symbol), timestamp_ms, float(high_price), on_duplicate="max")
                pipe.ts().add(self._5m_low_key(symbol), timestamp_ms, float(low_price), on_duplicate="min")
                pipe.ts().add(self._5m_close_key(symbol), timestamp_ms, float(close_price), on_duplicate="last")
                pipe.ts().add(self._5m_volume_key(symbol), timestamp_ms, float(volume), on_duplicate="sum")
                await pipe.execute()
        except ResponseError as e:
            err_str = str(e).lower()
            if "key does not exist" in err_str:
                await self.create_5m_timeseries(symbol)
                async with r.pipeline() as pipe:
                    pipe.ts().add(self._5m_open_key(symbol), timestamp_ms, float(open_price), on_duplicate="first")
                    pipe.ts().add(self._5m_high_key(symbol), timestamp_ms, float(high_price), on_duplicate="max")
                    pipe.ts().add(self._5m_low_key(symbol), timestamp_ms, float(low_price), on_duplicate="min")
                    pipe.ts().add(self._5m_close_key(symbol), timestamp_ms, float(close_price), on_duplicate="last")
                    pipe.ts().add(self._5m_volume_key(symbol), timestamp_ms, float(volume), on_duplicate="sum")
                    await pipe.execute()
            elif "timestamp cannot be older" in err_str:
                pass
            else:
                raise

    # ============================================================
    # PRIMARY CANDLE READING METHODS
    # ============================================================

    async def get_5m_candles(
        self,
        symbol: str,
        from_ts: Optional[int] = None,
        to_ts: Optional[int] = None,
    ) -> Dict[str, List]:
        """
        Get 5m candle data from downsampled keys.

        Args:
            symbol: Instrument symbol
            from_ts: Start timestamp in milliseconds (default: 18 hours ago)
            to_ts: End timestamp in milliseconds (default: now)

        Returns:
            Dict with keys: timestamp, open, high, low, close, volume
            Each value is a list, ordered by timestamp ascending.
        """
        r = self._get_client()

        if to_ts is None:
            to_ts = int(time.time() * 1000)
        if from_ts is None:
            from_ts = to_ts - self.CANDLE_5M_RETENTION_MS

        try:
            open_data, high_data, low_data, close_data, vol_data = await asyncio.gather(
                r.ts().range(self._5m_open_key(symbol), from_ts, to_ts),
                r.ts().range(self._5m_high_key(symbol), from_ts, to_ts),
                r.ts().range(self._5m_low_key(symbol), from_ts, to_ts),
                r.ts().range(self._5m_close_key(symbol), from_ts, to_ts),
                r.ts().range(self._5m_volume_key(symbol), from_ts, to_ts),
            )
        except ResponseError as e:
            if "key does not exist" in str(e).lower():
                return {"timestamp": [], "open": [], "high": [], "low": [], "close": [], "volume": []}
            raise

        def to_map(lst):
            return {int(ts): float(val) for ts, val in (lst or []) if ts is not None}

        o_map = to_map(open_data)
        h_map = to_map(high_data)
        l_map = to_map(low_data)
        c_map = to_map(close_data)
        v_map = to_map(vol_data)

        all_ts = sorted(set(o_map.keys()) | set(h_map.keys()) | set(l_map.keys()) | set(c_map.keys()) | set(v_map.keys()))

        timestamps, opens, highs, lows, closes, volumes = [], [], [], [], [], []

        for ts in all_ts:
            if ts not in o_map and ts not in c_map:
                continue
            timestamps.append(ts)
            opens.append(o_map.get(ts))
            highs.append(h_map.get(ts))
            lows.append(l_map.get(ts))
            closes.append(c_map.get(ts))
            volumes.append(v_map.get(ts, 0.0))

        return {"timestamp": timestamps, "open": opens, "high": highs, "low": lows, "close": closes, "volume": volumes}

    async def get_last_price(self, symbol: str) -> Optional[float]:
        """Get the latest price from the tick series."""
        r = self._get_client()
        try:
            # get returns (timestamp, value)
            res = await r.ts().get(self._tick_price_key(symbol))
            if res:
                return float(res[1])
        except ResponseError as e:
            if "key does not exist" not in str(e).lower():
                raise
        return None

    async def get_current_5m_candle(self, symbol: str) -> Optional[Dict[str, float]]:
        """
        Get the current ongoing 5m candle from raw tick data.
        This represents the incomplete candle being formed right now.

        Args:
            symbol: Instrument symbol

        Returns:
            Dict with keys: timestamp, open, high, low, close, volume, count
            Or None if no tick data available.
        """
        r = self._get_client()
        now = int(time.time() * 1000)

        # Current 5m bucket: floor to 5m boundary
        bucket_start = self._align_to_5m_slot(now, ceiling=False)
        bucket_end = bucket_start + self.BUCKET_5M_MS

        price_key = self._tick_price_key(symbol)
        vol_key = self._tick_volume_key(symbol)

        try:
            first, last, high, low, vol_min, vol_max, count = await asyncio.gather(
                r.ts().range(price_key, bucket_start, bucket_end, aggregation_type="first", bucket_size_msec=self.BUCKET_5M_MS),
                r.ts().range(price_key, bucket_start, bucket_end, aggregation_type="last", bucket_size_msec=self.BUCKET_5M_MS),
                r.ts().range(price_key, bucket_start, bucket_end, aggregation_type="max", bucket_size_msec=self.BUCKET_5M_MS),
                r.ts().range(price_key, bucket_start, bucket_end, aggregation_type="min", bucket_size_msec=self.BUCKET_5M_MS),
                r.ts().range(vol_key, bucket_start, bucket_end, aggregation_type="min", bucket_size_msec=self.BUCKET_5M_MS),
                r.ts().range(vol_key, bucket_start, bucket_end, aggregation_type="max", bucket_size_msec=self.BUCKET_5M_MS),
                r.ts().range(price_key, bucket_start, bucket_end, aggregation_type="count", bucket_size_msec=self.BUCKET_5M_MS),
            )
        except ResponseError as e:
            if "key does not exist" in str(e).lower():
                return None
            raise

        # Extract values from results (each is a list like [[ts, val]])
        def extract_val(result):
            if result and len(result) > 0 and len(result[0]) > 1:
                return float(result[0][1])
            return None

        open_val = extract_val(first)
        close_val = extract_val(last)
        high_val = extract_val(high)
        low_val = extract_val(low)
        vol_min_val = extract_val(vol_min) or 0.0
        vol_max_val = extract_val(vol_max) or 0.0
        count_val = int(extract_val(count) or 0)

        if open_val is None:
            return None

        return {
            "timestamp": bucket_start,
            "open": open_val,
            "high": high_val,
            "low": low_val,
            "close": close_val,
            "volume": max(0.0, vol_max_val - vol_min_val),
            "count": count_val,
        }

    async def get_current_daily_candle(
        self, symbol: str, day_start_ts: int
    ) -> Optional[Dict[str, float]]:
        """
        Get the current daily candle by aggregating all 5m candles from day_start_ts until now,
        plus the current ongoing 5m candle.

        Args:
            symbol: Instrument symbol
            day_start_ts: Start of trading day in milliseconds (e.g., market open time)

        Returns:
            Dict with keys: timestamp, open, high, low, close, volume
            Or None if no data available.
        """
        now = int(time.time() * 1000)

        # Get all completed 5m candles for today
        candles = await self.get_5m_candles(symbol, from_ts=day_start_ts, to_ts=now)

        # Get current ongoing candle
        current_candle = await self.get_current_5m_candle(symbol)

        # Combine data
        timestamps = candles.get("timestamp", [])
        opens = candles.get("open", [])
        highs = candles.get("high", [])
        lows = candles.get("low", [])
        closes = candles.get("close", [])
        volumes = candles.get("volume", [])

        # Add current candle if it exists and is not already in the list
        if current_candle:
            current_ts = current_candle["timestamp"]
            if current_ts not in timestamps:
                timestamps.append(current_ts)
                opens.append(current_candle["open"])
                highs.append(current_candle["high"])
                lows.append(current_candle["low"])
                closes.append(current_candle["close"])
                volumes.append(current_candle["volume"])

        if not timestamps:
            return None

        # Aggregate into daily candle
        valid_highs = [h for h in highs if h is not None]
        valid_lows = [l for l in lows if l is not None]
        valid_volumes = [v for v in volumes if v is not None]

        # Open: first non-None open value
        daily_open = next((o for o in opens if o is not None), None)
        # Close: last non-None close value
        daily_close = next((c for c in reversed(closes) if c is not None), None)

        return {
            "timestamp": day_start_ts,
            "open": daily_open,
            "high": max(valid_highs) if valid_highs else None,
            "low": min(valid_lows) if valid_lows else None,
            "close": daily_close,
            "volume": sum(valid_volumes) if valid_volumes else 0,
        }

    async def get_all_intraday_candles(
        self,
        symbol: str,
        from_ts: Optional[int] = None,
        to_ts: Optional[int] = None,
    ) -> Dict[str, List]:
        """
        Get all 5m candles including the current ongoing candle.
        This is the primary method for fetching intraday data for display.

        Args:
            symbol: Instrument symbol
            from_ts: Start timestamp in milliseconds
            to_ts: End timestamp in milliseconds (default: now)

        Returns:
            Dict with keys: timestamp, open, high, low, close, volume
            Includes the current ongoing candle merged at the end.
        """
        if to_ts is None:
            to_ts = int(time.time() * 1000)

        # Get completed 5m candles
        candles = await self.get_5m_candles(symbol, from_ts=from_ts, to_ts=to_ts)

        # Get current ongoing candle
        current_candle = await self.get_current_5m_candle(symbol)

        if current_candle:
            current_ts = current_candle["timestamp"]
            timestamps = candles.get("timestamp", [])

            # Check if current candle is within our time range
            if from_ts is None or current_ts >= from_ts:
                if current_ts in timestamps:
                    # Update existing entry with current data
                    idx = timestamps.index(current_ts)
                    candles["open"][idx] = current_candle["open"]
                    candles["high"][idx] = current_candle["high"]
                    candles["low"][idx] = current_candle["low"]
                    candles["close"][idx] = current_candle["close"]
                    candles["volume"][idx] = current_candle["volume"]
                else:
                    # Append current candle
                    candles["timestamp"].append(current_ts)
                    candles["open"].append(current_candle["open"])
                    candles["high"].append(current_candle["high"])
                    candles["low"].append(current_candle["low"])
                    candles["close"].append(current_candle["close"])
                    candles["volume"].append(current_candle["volume"])

        return candles

    # ============================================================
    # UTILITY METHODS
    # ============================================================

    async def get_latest_5m_timestamp(self, symbol: str) -> Optional[int]:
        """Get the latest timestamp in the 5m candle series for a symbol. Used for gap detection."""
        r = self._get_client()
        try:
            info = await r.ts().info(self._5m_close_key(symbol))
            if info and info.last_timestamp:
                return int(info.last_timestamp)
        except ResponseError as e:
            if "key does not exist" in str(e).lower():
                return None
            raise
        return None

    async def get_all_keys(self) -> List[str]:
        """Return all symbol keys that have tick time series in Redis."""
        r = self._get_client()
        keys = []

        try:
            tick_keys = await r.ts().queryindex(["type=tick", "field=price"])
            if tick_keys:
                for key in tick_keys:
                    key_str = key.decode("utf-8") if isinstance(key, bytes) else str(key)
                    if key_str.endswith(":tick:price"):
                        symbol = key_str[:-11]
                        keys.append(symbol)
        except Exception:
            pass

        if not keys:
            try:
                tick_keys = await r.keys("*:tick:price")
                for key in tick_keys:
                    key_str = key.decode("utf-8") if isinstance(key, bytes) else str(key)
                    if key_str.endswith(":tick:price"):
                        symbol = key_str[:-11]
                        keys.append(symbol)
            except Exception:
                return []

        return keys

    async def get_recordable_symbols(self) -> List[str]:
        """Return all symbols that have 5m downsampled keys (i.e., recordable instruments)."""
        r = self._get_client()
        keys = []

        try:
            close_keys = await r.ts().queryindex(["type=5m", "field=close"])
            if close_keys:
                for key in close_keys:
                    key_str = key.decode("utf-8") if isinstance(key, bytes) else str(key)
                    if key_str.endswith(":5m:close"):
                        symbol = key_str[:-9]
                        keys.append(symbol)
        except Exception:
            pass

        return keys

    async def delete_symbol_keys(self, symbol: str) -> None:
        """Delete all Redis keys for a symbol (tick and 5m)."""
        r = self._get_client()
        keys_to_delete = [
            self._tick_price_key(symbol),
            self._tick_volume_key(symbol),
            self._5m_open_key(symbol),
            self._5m_high_key(symbol),
            self._5m_low_key(symbol),
            self._5m_close_key(symbol),
            self._5m_volume_key(symbol),
            f"{symbol}:stats",
        ]
        try:
            await r.delete(*keys_to_delete)
        except Exception as e:
            logger.warning(f"Error deleting keys for {symbol}: {e}")


redis_ts = None


def get_redis_timeseries() -> RedisTimeSeries:
    global redis_ts

    if redis_ts is None:
        redis_ts = RedisTimeSeries()
    return redis_ts
