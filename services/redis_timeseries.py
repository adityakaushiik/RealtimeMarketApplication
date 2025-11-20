import asyncio
import time
from typing import Dict, List, Optional
from config.redis_config import get_redis
from redis.exceptions import ResponseError

from utils.common_constants import DataBroadcastFormat


class RedisTimeSeries:
    """
    Helper around RedisTimeSeries module to store tick data (price, volume) and query OHLCV.

    Keys created:
      <key>:price  - price ticks
      <key>:volume - size/volume per tick

    Retention: 15 minutes (older samples are auto-trimmed by Redis).
    """

    def __init__(self, retention_minutes: int = 15, redis_client=None) -> None:
        self.retention_ms = retention_minutes * 60 * 1000
        # Allow dependency injection of a redis client; else lazy init on first use
        self._redis = redis_client

    def _get_client(self):
        if self._redis is None:
            self._redis = get_redis()
        return self._redis

    def _align_to_5min_slot(self, timestamp_ms: int) -> int:
        """
        Align a timestamp to the nearest 5-minute slot boundary (floor).

        Example:
            10:07 -> 10:05
            10:03 -> 10:00
            9:58 -> 9:55

        Args:
            timestamp_ms: Timestamp in milliseconds

        Returns:
            Aligned timestamp in milliseconds (floor to nearest 5-min boundary)
        """
        five_min_ms = 5 * 60 * 1000
        return (timestamp_ms // five_min_ms) * five_min_ms

    async def create_timeseries(self, key: str) -> None:
        """Create price and volume time series for a symbol with retention of 15 minutes.
        If the series already exist, the calls are ignored.
        """
        r = self._get_client()
        price_key = f"{key}:price"
        vol_key = f"{key}:volume"
        try:
            await r.ts().create(
                price_key,
                retention_msecs=self.retention_ms,
                labels={"ts": "price", "key": key},
            )
        except ResponseError as e:
            # Ignore if key already exists
            if "key already exists" not in str(e).lower():
                raise
        try:
            await r.ts().create(
                vol_key,
                retention_msecs=self.retention_ms,
                labels={"ts": "volume", "key": key},
            )
        except ResponseError as e:
            if "key already exists" not in str(e).lower():
                raise

    async def add_to_timeseries(
        self, key: str, timestamp: float, price: float, volume: float = 0.0
    ) -> None:
        r = self._get_client()
        price_key = f"{key}:price"
        vol_key = f"{key}:volume"

        # Ensure series exist (idempotent create)
        await self.create_timeseries(key)

        # Add points
        await r.ts().add(price_key, timestamp, float(price))
        await r.ts().add(vol_key, timestamp, float(volume))

    async def get_from_timeseries(self, key: str) -> List[List]:
        """Back-compat helper: returns AVG over last 5 minutes for the price series in a single bucket.
        Prefer using get_ohlcv_last_5m or get_ohlcv_series.
        """
        now = int(time.time() * 1000)
        # Align to current 5-minute slot
        aligned_now = self._align_to_5min_slot(now)
        from_ts = aligned_now - 5 * 60 * 1000
        to_ts = aligned_now
        r = self._get_client()
        # Aggregate with AVG over a single 5-minute bucket, aligned to the 5-min slot
        result = await r.ts().range(
            f"{key}:price",
            from_ts,
            to_ts,
            aggregation_type="avg",
            bucket_size_msec=5 * 60 * 1000,
            align=to_ts,
        )
        return result

    async def get_ohlcv_last_5m(self, key: str) -> Optional[Dict[str, float]]:
        """Return the OHLCV for the previous 5 minutes as a single bucket aligned to current 5-min slot.

        Returns None if there is no data in the window.
        """
        now = int(time.time() * 1000)
        # Align to current 5-minute slot
        aligned_now = self._align_to_5min_slot(now)
        return await self.get_ohlcv_window(key, window_minutes=5, align_to_ts=aligned_now)

    async def get_last_traded_price(self, key: str) -> Optional[DataBroadcastFormat]:
        """Return the last traded price, timestamp, and volume for the given key.

        Uses TS.GET which is optimized for retrieving the latest sample.
        Returns a dict with timestamp, price, and volume, or None if no data exists.

        Returns:
            Dict with keys: 'timestamp', 'price', 'volume' or None
            Example: {"timestamp": 1700000000000, "price": 150.25, "volume": 1000}
        """
        r = self._get_client()
        price_key = f"{key}:price"
        vol_key = f"{key}:volume"

        try:
            # Fetch price and volume in parallel using TS.GET
            price_result, vol_result = await asyncio.gather(
                r.ts().get(price_key),
                r.ts().get(vol_key),
                return_exceptions=True
            )

            # Handle price result
            if isinstance(price_result, Exception):
                return None
            if not price_result or len(price_result) < 2:
                return None

            timestamp = int(price_result[0])
            price = float(price_result[1])

            # Handle volume result (may not exist or have errors)
            volume = 0.0
            if not isinstance(vol_result, Exception) and vol_result and len(vol_result) >= 2:
                volume = float(vol_result[1])

            return DataBroadcastFormat(
                timestamp=timestamp,
                symbol=key,
                price=price,
                volume=volume
            )

        except ResponseError as e:
            # Handle case where key doesn't exist
            if "key does not exist" in str(e).lower() or "TSDB: the key does not exist" in str(e):
                return None
            raise

    async def get_multiple_last_traded_prices(self, keys: List[str]) -> List[DataBroadcastFormat]:
        """Return last traded price, timestamp, and volume for multiple symbols.

        Performs concurrent TS.GET operations for each symbol's price and volume series.
        Any symbol without a valid latest price sample is skipped. Volume defaults to 0.0 if
        missing or errored.

        Args:
            keys: List of symbol keys (e.g. ["AAPL", "MSFT"]).

        Returns:
            List[DataBroadcastFormat] where each entry contains timestamp, symbol, price, volume.
        """
        if not keys:
            return []
        r = self._get_client()

        # Build tasks for prices and volumes
        price_tasks = [r.ts().get(f"{k}:price") for k in keys]
        volume_tasks = [r.ts().get(f"{k}:volume") for k in keys]

        price_results = await asyncio.gather(*price_tasks, return_exceptions=True)
        volume_results = await asyncio.gather(*volume_tasks, return_exceptions=True)

        results: List[DataBroadcastFormat] = []
        for key, p_res, v_res in zip(keys, price_results, volume_results):
            # Skip if price retrieval failed or invalid
            if isinstance(p_res, Exception) or not p_res or len(p_res) < 2:
                continue
            try:
                ts = int(p_res[0])
                price_val = float(p_res[1])
            except Exception:
                continue
            volume_val = 0.0
            if not isinstance(v_res, Exception) and v_res and len(v_res) >= 2:
                try:
                    volume_val = float(v_res[1])
                except Exception:
                    volume_val = 0.0
            results.append(
                DataBroadcastFormat(
                    timestamp=ts,
                    symbol=key,
                    price=price_val,
                    volume=volume_val,
                )
            )
        return results

    async def get_ohlcv_window(
        self,
        key: str,
        window_minutes: int,
        align_to_ts: Optional[int] = None,
    ) -> Optional[Dict[str, float]]:
        """Return OHLCV for the last window_minutes as a single bucket aligned to align_to_ts (default now)."""
        if align_to_ts is None:
            align_to_ts = int(time.time() * 1000)
        to_ts = align_to_ts
        from_ts = to_ts - window_minutes * 60 * 1000
        bucket = window_minutes * 60 * 1000
        ohlcv_data = await self.get_ohlcv_series(
            key,
            window_minutes=window_minutes,
            bucket_minutes=window_minutes,
            align_to_ts=to_ts,
        )
        if not ohlcv_data or not ohlcv_data.get("timestamp"):
            return None
        # Since bucket equals window, we expect a single entry - return the last one
        idx = -1
        return {
            "ts": ohlcv_data["timestamp"][idx],
            "open": ohlcv_data["open"][idx],
            "high": ohlcv_data["high"][idx],
            "low": ohlcv_data["low"][idx],
            "close": ohlcv_data["close"][idx],
            "volume": ohlcv_data["volume"][idx],
        }

    async def get_ohlcv_series(
        self,
        key: str,
        window_minutes: int = 15,
        bucket_minutes: int = 5,
        align_to_ts: Optional[int] = None,
    ) -> Dict[str, List[float]]:
        """
        Return OHLCV buckets for the previous window_minutes, aggregated in bucket_minutes buckets.

        All timestamps are aligned to 5-minute slot boundaries. For example, if current time is 10:07:
        - The current slot is 10:05
        - Previous 3 candles would be: 10:05, 10:00, 9:55

        Returns data in columnar format for better performance and smaller payload:
        {
            "timestamp": [t1, t2, t3, ...],
            "open": [o1, o2, o3, ...],
            "high": [h1, h2, h3, ...],
            "low": [l1, l2, l3, ...],
            "close": [c1, c2, c3, ...],
            "volume": [v1, v2, v3, ...]
        }
        """
        if align_to_ts is None:
            align_to_ts = int(time.time() * 1000)

        # Align to the current 5-minute slot
        aligned_ts = self._align_to_5min_slot(align_to_ts)
        to_ts = aligned_ts
        from_ts = to_ts - window_minutes * 60 * 1000
        bucket = bucket_minutes * 60 * 1000

        r = self._get_client()
        price_key = f"{key}:price"
        vol_key = f"{key}:volume"

        # Query aggregations for the same buckets (aligned to the end time) in parallel
        first, last, high, low, vol = await asyncio.gather(
            r.ts().range(
                price_key,
                from_ts,
                to_ts,
                aggregation_type="first",
                bucket_size_msec=bucket,
                align=to_ts,
            ),
            r.ts().range(
                price_key,
                from_ts,
                to_ts,
                aggregation_type="last",
                bucket_size_msec=bucket,
                align=to_ts,
            ),
            r.ts().range(
                price_key,
                from_ts,
                to_ts,
                aggregation_type="max",
                bucket_size_msec=bucket,
                align=to_ts,
            ),
            r.ts().range(
                price_key,
                from_ts,
                to_ts,
                aggregation_type="min",
                bucket_size_msec=bucket,
                align=to_ts,
            ),
            r.ts().range(
                vol_key,
                from_ts,
                to_ts,
                aggregation_type="sum",
                bucket_size_msec=bucket,
                align=to_ts,
            ),
        )

        # Convert lists like [[ts, val], ...] into a dict keyed by ts
        def to_map(lst):
            m = {}
            for item in lst or []:
                if not item:
                    continue
                ts, val = item
                try:
                    ts_int = int(ts)
                except Exception:
                    # When decode_responses=True, ts may already be int/str
                    ts_int = int(ts)
                m[ts_int] = float(val)
            return m

        o_map = to_map(first)
        h_map = to_map(high)
        l_map = to_map(low)
        c_map = to_map(last)
        v_map = to_map(vol)

        # Build a sorted list of bucket end timestamps (union of keys present)
        all_ts = sorted(
            set(o_map.keys())
            | set(h_map.keys())
            | set(l_map.keys())
            | set(c_map.keys())
            | set(v_map.keys())
        )

        # Build columnar format for better performance and smaller payload
        # Format: { timestamp: [t1, t2, ...], open: [o1, o2, ...], ... }
        timestamps = []
        opens = []
        highs = []
        lows = []
        closes = []
        volumes = []

        for ts in all_ts:
            # Only include buckets where we at least have O and C; H/L/V may be absent if no data
            if ts not in o_map and ts not in c_map:
                continue
            timestamps.append(ts)
            opens.append(o_map.get(ts))
            highs.append(h_map.get(ts))
            lows.append(l_map.get(ts))
            closes.append(c_map.get(ts))
            volumes.append(v_map.get(ts, 0.0))

        return {
            "timestamp": timestamps,
            "open": opens,
            "high": highs,
            "low": lows,
            "close": closes,
            "volume": volumes,
        }
