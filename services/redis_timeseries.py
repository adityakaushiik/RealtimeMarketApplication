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
      <key>:volume - size/volume per tick (cumulative volume, stored as last value)
      <key>:volume_delta - volume delta per tick (for accurate aggregation)

    Retention: 60 minutes (extended from 15 for robustness).
    """

    def __init__(self, retention_minutes: int = 60, redis_client=None) -> None:
        self.retention_ms = retention_minutes * 60 * 1000
        # Track last known cumulative volume for delta calculation (B2)
        self._last_cumulative_volume: Dict[str, int] = {}
        # Allow dependency injection of a redis client; else lazy init on first use
        self._redis = redis_client

    def _get_client(self):
        if self._redis is None:
            self._redis = get_redis()
        return self._redis

    def _align_to_interval_slot(
        self, timestamp_ms: int, interval_minutes: int = 5, ceiling: bool = False
    ) -> int:
        """
        Align a timestamp to the nearest interval slot boundary.

        Args:
            timestamp_ms: Timestamp in milliseconds
            interval_minutes: Interval in minutes (e.g., 1, 5, 15)
            ceiling: If True, align to next slot (ceiling); if False, align to current slot (floor)

        Returns:
            Aligned timestamp in milliseconds
        """
        interval_ms = interval_minutes * 60 * 1000
        if ceiling:
            # Ceiling: round up to next interval boundary (unless already on boundary)
            remainder = timestamp_ms % interval_ms
            if remainder == 0:
                return timestamp_ms
            return timestamp_ms + (interval_ms - remainder)
        else:
            # Floor: round down to current interval boundary
            return (timestamp_ms // interval_ms) * interval_ms

    async def create_timeseries(self, key: str) -> None:
        """Create price and volume time series for a symbol with retention of 15 minutes.
        If the series already exist, the calls are ignored.
        """
        r = self._get_client()
        price_key = f"{key}:price"
        vol_key = f"{key}:volume"

        # Use pipeline to create both keys in one go
        async with r.pipeline() as pipe:
            try:
                pipe.ts().create(
                    price_key,
                    retention_msecs=self.retention_ms,
                    labels={"ts": "price", "key": key},
                    duplicate_policy="last",
                )
                pipe.ts().create(
                    vol_key,
                    retention_msecs=self.retention_ms,
                    labels={"ts": "volume", "key": key},
                    duplicate_policy="sum",
                )
                await pipe.execute()
            except ResponseError as e:
                # Ignore "key already exists" errors, raise others
                if "key already exists" not in str(e).lower():
                    raise

    async def add_to_timeseries(
        self, key: str, timestamp: float, price: float, volume: float = 0.0
    ) -> None:
        """
        Add tick data to timeseries.

        B2: Volume handling - if volume appears to be cumulative (larger than previous),
        calculate delta for accurate aggregation. Otherwise treat as incremental.
        """
        r = self._get_client()
        price_key = f"{key}:price"
        vol_key = f"{key}:volume"

        # B2: Calculate volume delta for cumulative volumes
        volume_to_store = float(volume)
        last_vol = self._last_cumulative_volume.get(key, 0)

        if volume > 0:
            # If volume is greater than last known, treat as cumulative and calc delta
            if volume > last_vol:
                volume_to_store = float(volume - last_vol)
                self._last_cumulative_volume[key] = int(volume)
            elif volume < last_vol * 0.1:
                # Volume much smaller likely means new day/reset - use as-is
                volume_to_store = float(volume)
                self._last_cumulative_volume[key] = int(volume)
            # else: volume same or slightly less, use as incremental

        # Optimistic add: try to add first. If it fails because key doesn't exist, create and retry.
        try:
            async with r.pipeline() as pipe:
                pipe.ts().add(price_key, timestamp, float(price), on_duplicate="last")
                pipe.ts().add(vol_key, timestamp, volume_to_store, on_duplicate="sum")
                await pipe.execute()
        except ResponseError as e:
            err_str = str(e).lower()
            if "key does not exist" in err_str:
                await self.create_timeseries(key)
                async with r.pipeline() as pipe:
                    pipe.ts().add(price_key, timestamp, float(price), on_duplicate="last")
                    pipe.ts().add(vol_key, timestamp, volume_to_store, on_duplicate="sum")
                    await pipe.execute()
            elif "timestamp cannot be older" in err_str:
                pass  # Ignore out-of-order data
            else:
                raise

    async def get_ohlcv_last_5m(self, key: str) -> Optional[Dict[str, float]]:
        """Return the OHLCV for the last 5 minutes as a single bucket, including the ongoing candle."""
        now = int(time.time() * 1000)
        # Align to next 5-minute slot (ceiling) to include ongoing candle
        aligned_now = self._align_to_interval_slot(
            now, interval_minutes=5, ceiling=True
        )
        return await self.get_ohlcv_window(
            key, window_minutes=5, align_to_ts=aligned_now
        )

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
                r.ts().get(price_key), r.ts().get(vol_key), return_exceptions=True
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
            if (
                not isinstance(vol_result, Exception)
                and vol_result
                and len(vol_result) >= 2
            ):
                volume = float(vol_result[1])

            return DataBroadcastFormat(
                timestamp=timestamp, symbol=key, price=price, volume=volume
            )

        except ResponseError as e:
            # Handle case where key doesn't exist
            if "key does not exist" in str(
                e
            ).lower() or "TSDB: the key does not exist" in str(e):
                return None
            raise

    async def get_multiple_last_traded_prices(
        self, keys: List[str]
    ) -> List[DataBroadcastFormat]:
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

        # Use pipeline to batch all GET commands in a single RTT
        async with r.pipeline() as pipe:
            for k in keys:
                pipe.ts().get(f"{k}:price")
                pipe.ts().get(f"{k}:volume")

            # Execute all commands in one batch
            # Results will be [price1, vol1, price2, vol2, ...]
            # raise_on_error=False ensures that if one key is missing, the whole pipeline doesn't fail
            results = await pipe.execute(raise_on_error=False)

        broadcast_data: List[DataBroadcastFormat] = []

        # Iterate through results in pairs (price, volume)
        for i in range(0, len(results), 2):
            key = keys[i // 2]
            p_res = results[i]
            v_res = results[i + 1]

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

            broadcast_data.append(
                DataBroadcastFormat(
                    timestamp=ts,
                    symbol=key,
                    price=price_val,
                    volume=volume_val,
                )
            )

        return broadcast_data

    async def get_ohlcv_window(
        self,
        key: str,
        window_minutes: int,
        align_to_ts: Optional[int] = None,
        bucket_minutes: Optional[int] = None,
    ) -> Optional[Dict[str, float]]:
        """Return OHLCV for the last window_minutes as a single bucket aligned to align_to_ts (default now)."""
        if align_to_ts is None:
            align_to_ts = int(time.time() * 1000)

        if bucket_minutes is None:
            bucket_minutes = window_minutes

        to_ts = align_to_ts
        from_ts = to_ts - window_minutes * 60 * 1000

        ohlcv_data = await self.get_ohlcv_series(
            key,
            window_minutes=window_minutes,
            bucket_minutes=bucket_minutes,
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
            "count": ohlcv_data.get("count", [0])[idx] if ohlcv_data.get("count") else 0,
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

        All timestamps are aligned to bucket_minutes slot boundaries. For example, if current time is 10:08 and bucket_minutes=5:
        - The query range end aligns to 10:10 (next slot, ceiling)
        - With 15 min window, query from 9:55 to 10:10
        - This returns 3 candles: 10:05 (ongoing), 10:00, 9:55

        For 1-minute buckets at 10:07:30:
        - Aligns to 10:08 (next minute, ceiling)
        - With 15 min window, query from 9:53 to 10:08
        - Returns 15 one-minute candles

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

        # Align to the NEXT bucket interval slot (ceiling) to include the ongoing candle
        aligned_ts = self._align_to_interval_slot(
            align_to_ts, interval_minutes=bucket_minutes, ceiling=True
        )
        to_ts = aligned_ts
        from_ts = to_ts - window_minutes * 60 * 1000
        bucket = bucket_minutes * 60 * 1000

        r = self._get_client()
        price_key = f"{key}:price"
        vol_key = f"{key}:volume"

        try:
            # Query aggregations for the same buckets (aligned to the end time) in parallel
            # logger.debug(f"TS.RANGE {price_key} {from_ts} {to_ts} bucket={bucket} align={to_ts}")
            first, last, high, low, vol, count = await asyncio.gather(
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
                r.ts().range(
                    price_key,
                    from_ts,
                    to_ts,
                    aggregation_type="count",
                    bucket_size_msec=bucket,
                    align=to_ts,
                ),
            )
        except ResponseError as e:
            # Handle case where key might have been deleted or expired
            if "key does not exist" in str(
                e
            ).lower() or "TSDB: the key does not exist" in str(e):
                return {
                    "timestamp": [],
                    "open": [],
                    "high": [],
                    "low": [],
                    "close": [],
                    "volume": [],
                    "count": [],
                }
            raise

        # if not first:
        #    logger.debug(f"Empty result for {price_key} range {from_ts}-{to_ts}")

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
        cnt_map = to_map(count)

        # Build a sorted list of bucket end timestamps (union of keys present)
        all_ts = sorted(
            set(o_map.keys())
            | set(h_map.keys())
            | set(l_map.keys())
            | set(c_map.keys())
            | set(v_map.keys())
            | set(cnt_map.keys())
        )

        # Build columnar format for better performance and smaller payload
        # Format: { timestamp: [t1, t2, ...], open: [o1, o2, ...], ... }
        timestamps = []
        opens = []
        highs = []
        lows = []
        closes = []
        volumes = []
        counts = []

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
            counts.append(int(cnt_map.get(ts, 0)))

        return {
            "timestamp": timestamps,
            "open": opens,
            "high": highs,
            "low": lows,
            "close": closes,
            "volume": volumes,
            "count": counts,
        }

    async def get_all_ohlcv_last_5m(
        self, keys: List[str], interval_minutes: int = 5
    ) -> Dict[str, Optional[Dict[str, float]]]:
        """Return OHLCV for the last N minutes for multiple symbols.

        Args:
            keys: List of symbol keys
            interval_minutes: Interval in minutes for OHLCV aggregation (default: 5)
                Supports 1, 5, 15, or any custom minute interval

        Returns:
            Dictionary mapping symbol to OHLCV data or None
        """

        results: Dict[str, Optional[Dict[str, float]]] = {}

        # Use get_ohlcv_window to support custom intervals
        now = int(time.time() * 1000)
        # Align to the interval slot (e.g., 1-min, 5-min, or 15-min)
        aligned_now = self._align_to_interval_slot(
            now, interval_minutes=interval_minutes, ceiling=True
        )

        tasks = [
            self.get_ohlcv_window(
                key, window_minutes=interval_minutes, align_to_ts=aligned_now
            )
            for key in keys
        ]
        ohlcv_list = await asyncio.gather(*tasks)
        for key, ohlcv in zip(keys, ohlcv_list):
            results[key] = ohlcv
        return results

    async def get_all_keys(self) -> List[str]:
        """Return all symbol keys that have price time series in Redis."""

        r = self._get_client()
        keys = []

        # Use TS.QUERYINDEX to find all keys with label ts=price
        try:
            # TS.QUERYINDEX returns a list of keys matching the filter
            price_keys = await r.ts().queryindex(["ts=price"])

            if price_keys:
                for key in price_keys:
                    # Handle both string and bytes
                    key_str = (
                        key.decode("utf-8") if isinstance(key, bytes) else str(key)
                    )
                    if key_str.endswith(":price"):
                        symbol = key_str[:-6]  # Remove ":price" suffix
                        keys.append(symbol)
        except Exception as e:
            # Log error but continue to fallback
            pass

        # Fallback to KEYS pattern matching if TS.QUERYINDEX returned nothing or failed
        if not keys:
            try:
                price_keys = await r.keys("*:price")
                for key in price_keys:
                    key_str = (
                        key.decode("utf-8") if isinstance(key, bytes) else str(key)
                    )
                    if key_str.endswith(":price"):
                        symbol = key_str[:-6]
                        keys.append(symbol)
            except Exception as fallback_error:
                # If both methods fail, return empty list
                return []

        # Filter out any keys that might be provider-specific search codes if they differ from internal symbols
        # This is tricky because Redis doesn't know about internal vs provider symbols.
        # However, the DataSaver logic relies on these keys being mappable to internal symbols.
        # If the keys in Redis are provider search codes (e.g. "RELIANCE-EQ"), but our DB expects "RELIANCE",
        # then DataSaver will fail to map them.
        # Ideally, Redis keys should be INTERNAL symbols.

        return keys

    async def set_daily_stats(self, key: str, stats: Dict[str, float]) -> None:
        """
        Set daily statistics (e.g., previous close) for a symbol in a Redis Hash.
        Key: <symbol>:stats
        """
        r = self._get_client()
        stats_key = f"{key}:stats"
        # Set with expiration (e.g., 24 hours) to avoid stale data if symbol becomes inactive
        async with r.pipeline() as pipe:
            pipe.hset(stats_key, mapping=stats)
            pipe.expire(stats_key, 24 * 60 * 60)  # 24 hours
            await pipe.execute()

    async def get_daily_stats(self, key: str) -> Dict[str, float]:
        """
        Get daily statistics for a symbol.
        """
        r = self._get_client()
        stats_key = f"{key}:stats"
        return await r.hgetall(stats_key)


redis_ts = None


def get_redis_timeseries() -> RedisTimeSeries:
    global redis_ts

    if redis_ts is None:
        redis_ts = RedisTimeSeries()
    return redis_ts
