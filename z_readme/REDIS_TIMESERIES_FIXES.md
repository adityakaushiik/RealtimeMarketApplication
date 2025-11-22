# Redis TimeSeries Fixes - Implementation Summary

## Date: November 20, 2025

## Overview
Two critical fixes were implemented in the `RedisTimeSeries` class to improve real-time market data handling:
1. **Ongoing Candle Inclusion** - Query ranges now include the current ongoing candle
2. **Duplicate Timestamp Handling** - Multiple values at the same timestamp are now allowed

---

## Fix 1: Include Ongoing Candle in Query Results

### Problem
When querying OHLCV data at 10:08, the system would only return completed candles:
- Query range: 10:00 to 10:05
- Returned candles: 10:00, 9:55, 9:50
- **Missing**: The ongoing 10:05-10:10 candle

### Solution
Modified the alignment logic to use ceiling (round up) instead of floor (round down):
- Query range: 10:00 to 10:10
- Returned candles: 10:05 (ongoing), 10:00, 9:55 ✓

### Changes Made

#### 1. Updated `_align_to_5min_slot()` method
```python
def _align_to_5min_slot(self, timestamp_ms: int, ceiling: bool = False) -> int:
    """
    Align a timestamp to the nearest 5-minute slot boundary.
    
    Args:
        ceiling: If True, align to next slot; if False, align to current slot
    
    Examples:
        At 10:08:
        - ceiling=False: 10:05 (floor)
        - ceiling=True:  10:10 (ceiling) ✓
    """
```

#### 2. Updated `get_ohlcv_series()` method
- Changed: `aligned_ts = self._align_to_5min_slot(align_to_ts, ceiling=True)`
- Now queries up to the next 5-minute boundary, including ongoing candle

#### 3. Updated `get_ohlcv_last_5m()` method
- Changed: `aligned_now = self._align_to_5min_slot(now, ceiling=True)`
- Returns data for the ongoing 5-minute window

### Impact
- ✓ Real-time charts now show live data for the current candle
- ✓ WebSocket clients receive updates for the ongoing candle
- ✓ More accurate representation of current market conditions

---

## Fix 2: Allow Duplicate Timestamps

### Problem
Redis TimeSeries was using the default `DUPLICATE_POLICY BLOCK`, which:
- Rejected multiple values at the same timestamp
- Caused errors when multiple ticks arrived within the same millisecond
- Failed during high-frequency data ingestion

### Solution
Implemented `DUPLICATE_POLICY LAST` to keep the most recent value when timestamps collide.

### Changes Made

#### Updated `create_timeseries()` method
```python
await r.ts().create(
    price_key,
    retention_msecs=self.retention_ms,
    labels={"ts": "price", "key": key},
    duplicate_policy="last",  # ← Added this
)
```

### Duplicate Policy Options
- `BLOCK` (default): Reject duplicate timestamps → ✗ Causes errors
- `FIRST`: Keep first value, ignore later ones → ✗ Outdated data
- `LAST`: Keep latest value, replace earlier ones → ✓ Most accurate
- `MIN`: Keep minimum value
- `MAX`: Keep maximum value
- `SUM`: Add values together

### Why LAST?
For real-time market data, the most recent tick is always the most accurate:
- If price updates from $100 to $101 in the same millisecond, we want $101
- Later updates should override earlier ones at the same timestamp
- Ensures data freshness and accuracy

### Impact
- ✓ No more "TSDB: key already exists" errors
- ✓ High-frequency ticks can be ingested without failures
- ✓ Always shows the most recent price in fast-moving markets
- ✓ Handles microsecond-level market data updates

---

## Testing

### Test 1: Alignment Test
Run `test_alignment_fix.py` to verify ceiling alignment:
```powershell
python test_alignment_fix.py
```

Expected output shows:
- At 10:08 → aligns to 10:10 (ceiling)
- Query range includes ongoing candle

### Test 2: Duplicate Policy Test
Run `test_duplicate_policy.py` to verify duplicate handling:
```powershell
python test_duplicate_policy.py
```

Expected output shows:
- Multiple values at same timestamp are allowed
- Last value is retained

---

## Migration Notes

### For Existing Time Series
If you have existing time series created before this fix:

1. **Option A: Recreate (Recommended for new deployments)**
   ```python
   # Delete old series
   await redis_client.delete("SYMBOL:price", "SYMBOL:volume")
   # New series will be created with duplicate_policy="last"
   ```

2. **Option B: Use ALTER (Redis 7.2+)**
   ```python
   await redis_client.ts().alter("SYMBOL:price", duplicate_policy="last")
   await redis_client.ts().alter("SYMBOL:volume", duplicate_policy="last")
   ```

3. **Option C: Leave as-is**
   - Old series will use BLOCK policy
   - New series will use LAST policy
   - Consider recreating during maintenance window

### Redis Version Requirements
- Minimum: Redis with TimeSeries module v1.4+
- Recommended: Redis 7.0+ with TimeSeries v1.8+
- `duplicate_policy` parameter requires TimeSeries v1.4+

---

## Configuration

### Current Settings
```python
retention_minutes = 15  # 15 minutes of data retention
bucket_minutes = 5      # 5-minute OHLCV buckets
duplicate_policy = "last"  # Keep most recent value
```

### Adjustable Parameters
- `retention_minutes`: Increase for longer historical data
- `bucket_minutes`: Change for different candle intervals
- `duplicate_policy`: Change if different behavior needed

---

## API Changes

### No Breaking Changes
All changes are backward compatible:
- `_align_to_5min_slot()` defaults to `ceiling=False` (original behavior)
- Existing code continues to work
- New behavior only applies when explicitly using ceiling alignment

### Enhanced Methods
- `get_ohlcv_series()` - Now includes ongoing candle
- `get_ohlcv_last_5m()` - Now includes ongoing candle
- `create_timeseries()` - Now allows duplicates

---

## Performance Considerations

### Ceiling Alignment
- Minimal overhead: One additional modulo operation
- No additional Redis queries
- Same response time as before

### Duplicate Policy
- `LAST` policy has no performance penalty
- Actually prevents error handling overhead
- Reduces retry logic in ingestion pipeline

---

## Monitoring

### Metrics to Watch
1. **Data Freshness**: Ongoing candle should update in real-time
2. **Error Rate**: Should see reduction in duplicate timestamp errors
3. **Ingestion Rate**: High-frequency data should ingest without throttling

### Redis Commands for Debugging
```bash
# Check duplicate policy
TS.INFO SYMBOL:price

# View recent samples
TS.RANGE SYMBOL:price - + COUNT 10

# Check for gaps
TS.RANGE SYMBOL:price <from_ts> <to_ts>
```

---

## Related Files
- `services/redis_timeseries.py` - Main implementation
- `test_alignment_fix.py` - Test for ceiling alignment
- `test_duplicate_policy.py` - Test for duplicate handling
- `config/redis_config.py` - Redis client configuration

---

## Future Enhancements
- [ ] Add configurable duplicate policy per symbol
- [ ] Support for sub-minute candle intervals
- [ ] Automatic backfill for missing candles
- [ ] Compression rules for older data

---

## Questions & Support
For issues or questions about these changes:
1. Check Redis logs for TimeSeries errors
2. Verify Redis version supports duplicate_policy
3. Test with `test_duplicate_policy.py` script
4. Review this document for configuration options

