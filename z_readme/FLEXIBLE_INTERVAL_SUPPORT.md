# Flexible Interval Alignment Support

## Summary of Changes

The `RedisTimeSeries` class has been updated to support flexible interval alignment for 1-minute, 5-minute, 15-minute, and any custom minute intervals.

## Key Changes

### 1. **New Method: `_align_to_interval_slot()`**
Replaces the hardcoded 5-minute alignment with a flexible interval-based alignment.

```python
def _align_to_interval_slot(self, timestamp_ms: int, interval_minutes: int = 5, ceiling: bool = False) -> int:
    """
    Align a timestamp to the nearest interval slot boundary.
    
    Args:
        timestamp_ms: Timestamp in milliseconds
        interval_minutes: Interval in minutes (e.g., 1, 5, 15)
        ceiling: If True, align to next slot; if False, align to current slot
    """
```

### 2. **Backward Compatibility**
The old `_align_to_5min_slot()` method is kept as a wrapper for backward compatibility:

```python
def _align_to_5min_slot(self, timestamp_ms: int, ceiling: bool = False) -> int:
    """DEPRECATED: Use _align_to_interval_slot instead."""
    return self._align_to_interval_slot(timestamp_ms, interval_minutes=5, ceiling=ceiling)
```

### 3. **Updated Methods**

#### `get_all_ohlcv_last_5m(keys, interval_minutes=5)`
Now accepts an `interval_minutes` parameter to support custom intervals:

```python
# 1-minute candles
data = await redis_ts.get_all_ohlcv_last_5m(keys, interval_minutes=1)

# 5-minute candles (default)
data = await redis_ts.get_all_ohlcv_last_5m(keys, interval_minutes=5)

# 15-minute candles
data = await redis_ts.get_all_ohlcv_last_5m(keys, interval_minutes=15)
```

#### `get_ohlcv_series(key, window_minutes, bucket_minutes)`
Now aligns to the `bucket_minutes` interval automatically:

```python
# 15 minutes of data in 1-minute buckets
data = await redis_ts.get_ohlcv_series(key, window_minutes=15, bucket_minutes=1)

# 15 minutes of data in 5-minute buckets
data = await redis_ts.get_ohlcv_series(key, window_minutes=15, bucket_minutes=5)

# 60 minutes of data in 15-minute buckets
data = await redis_ts.get_ohlcv_series(key, window_minutes=60, bucket_minutes=15)
```

## Integration with DataSaver

The `DataSaver` class automatically uses the correct interval alignment:

### Example 1: 1-Minute Saves
```python
# In main.py
await data_saver.start_all_exchanges(interval_minutes=1)

# Data will be saved every 1 minute at:
# 09:16, 09:17, 09:18, ..., 15:30
```

### Example 2: 5-Minute Saves (Default)
```python
# In main.py
await data_saver.start_all_exchanges(interval_minutes=5)

# Data will be saved every 5 minutes at:
# 09:15, 09:20, 09:25, ..., 15:30
```

### Example 3: 15-Minute Saves
```python
# In main.py
await data_saver.start_all_exchanges(interval_minutes=15)

# Data will be saved every 15 minutes at:
# 09:15, 09:30, 09:45, ..., 15:30
```

## Alignment Examples

### 1-Minute Alignment
| Current Time | Floor Aligned | Ceiling Aligned |
|--------------|---------------|-----------------|
| 10:07:30     | 10:07:00      | 10:08:00        |
| 10:03:45     | 10:03:00      | 10:04:00        |
| 10:00:00     | 10:00:00      | 10:00:00        |

### 5-Minute Alignment
| Current Time | Floor Aligned | Ceiling Aligned |
|--------------|---------------|-----------------|
| 10:07:00     | 10:05:00      | 10:10:00        |
| 10:03:00     | 10:00:00      | 10:05:00        |
| 10:00:00     | 10:00:00      | 10:00:00        |

### 15-Minute Alignment
| Current Time | Floor Aligned | Ceiling Aligned |
|--------------|---------------|-----------------|
| 10:07:00     | 10:00:00      | 10:15:00        |
| 10:03:00     | 10:00:00      | 10:15:00        |
| 10:00:00     | 10:00:00      | 10:00:00        |

## Benefits

1. **Flexibility**: Support any minute interval (1, 5, 15, 30, 60, etc.)
2. **Consistency**: All OHLCV methods use the same alignment logic
3. **Backward Compatible**: Existing code continues to work
4. **Optimized**: Uses ceiling alignment to include ongoing candles

## Usage in main.py

```python
# Initialize DataSaver with desired interval
data_saver = DataSaver()

# Add exchanges
for exchange in exchanges:
    exchange_data = ExchangeData(
        exchange_name=exchange.name,
        exchange_id=exchange.id,
        market_open_time_hhmm=exchange.market_open_time,
        market_close_time_hhmm=exchange.market_close_time,
        timezone_str=exchange.timezone,
    )
    data_saver.add_exchange(exchange_data)

# Start with custom interval (1, 5, or 15 minutes)
await data_saver.start_all_exchanges(interval_minutes=5)
```

## Testing

Run the test script to verify alignment:
```bash
python z_tests/test_interval_alignment.py
```

## Notes

- **1-minute intervals**: Best for high-frequency trading, generates most records
- **5-minute intervals**: Default, good balance between granularity and storage
- **15-minute intervals**: Lower storage, still captures trends
- All intervals align to exact time boundaries for consistency
- Ceiling alignment ensures ongoing candles are included in queries

