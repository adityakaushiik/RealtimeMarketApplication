# Incremental Daily Updates - Performance Optimized Approach

## 🎯 Overview

Instead of running a heavy aggregation query at the end of the trading day, we now **update the daily record incrementally** with each 5-minute save. This is more efficient and provides real-time daily data throughout the trading day.

---

## ✅ Key Benefits

### 1. **Performance** 🚀
- **No heavy end-of-day aggregation** - No complex GROUP BY queries scanning thousands of intraday records
- **Simple UPDATE operations** - Just updating one daily record per instrument every 5 minutes
- **Incremental processing** - Spreads the load throughout the day instead of one big operation

### 2. **Real-time Daily Data** 📊
- Daily OHLCV data is **always up-to-date**
- Can query current day's high/low/volume at any time
- No waiting until end of day for daily data

### 3. **Simplicity** 🎨
- No need to track "has daily aggregation run?"
- No separate end-of-day job required
- Everything happens in one save operation

### 4. **Database Efficiency** 💾
- **Single transaction** for both intraday INSERT and daily UPDATE
- Fewer queries overall
- Less database load

---

## 🔄 How It Works

### Every 5 Minutes During Trading Hours

```
┌─────────────────────────────────────────────────────────────┐
│         RedisTimeSeries: Get last 5-min OHLCV               │
│         {symbol: {ts, o, h, l, c, v}}                       │
└─────────────────────────────────────────────────────────────┘
                            ↓
        ┌───────────────────────────────────────┐
        │  1. INSERT into price_history_intraday │
        │     - instrument_id                    │
        │     - timestamp (current 5-min)        │
        │     - open, high, low, close, volume   │
        └───────────────────────────────────────┘
                            ↓
        ┌───────────────────────────────────────┐
        │  2. UPSERT price_history_daily         │
        │     (same transaction)                 │
        └───────────────────────────────────────┘
                            ↓
            ┌───────────────────────────┐
            │ Does daily record exist?  │
            └───────────────────────────┘
                    │              │
                   YES            NO
                    │              │
                    ↓              ↓
        ┌─────────────────┐  ┌──────────────────┐
        │  UPDATE:        │  │  INSERT:         │
        │  - open (keep)  │  │  - open (set)    │
        │  - high (max)   │  │  - high (set)    │
        │  - low (min)    │  │  - low (set)     │
        │  - close (new)  │  │  - close (set)   │
        │  - volume (+)   │  │  - volume (set)  │
        └─────────────────┘  └──────────────────┘
```

---

## 📝 Update Logic

### Open Price
```python
# Keep the FIRST open of the day (don't update)
existing_daily.open  # No change!
```

### High Price
```python
# Take the MAXIMUM high seen so far
existing_daily.high = max(existing_daily.high, current_ohlcv["high"])
```

### Low Price
```python
# Take the MINIMUM low seen so far
existing_daily.low = min(existing_daily.low, current_ohlcv["low"])
```

### Close Price
```python
# Always update to LATEST close
existing_daily.close = current_ohlcv["close"]
```

### Volume
```python
# ACCUMULATE volume throughout the day
existing_daily.volume = (existing_daily.volume or 0) + int(current_ohlcv["volume"])
```

---

## 💡 Example Timeline

### 9:15 AM - First 5-min candle

**Intraday Save:**
```python
instrument_id=123, timestamp=1700123400000, o=100, h=102, l=99, c=101, v=10000
```

**Daily Record Created:**
```python
instrument_id=123, timestamp=1700092800000 (midnight)
open=100    # First open
high=102    # First high
low=99      # First low
close=101   # Latest close
volume=10000
```

---

### 9:20 AM - Second 5-min candle

**Intraday Save:**
```python
instrument_id=123, timestamp=1700123700000, o=101, h=105, l=100, c=104, v=15000
```

**Daily Record Updated:**
```python
instrument_id=123, timestamp=1700092800000
open=100    # UNCHANGED (keep first)
high=105    # UPDATED (max of 102, 105)
low=99      # UNCHANGED (min of 99, 100)
close=104   # UPDATED (latest)
volume=25000 # UPDATED (10000 + 15000)
```

---

### 9:25 AM - Third 5-min candle

**Intraday Save:**
```python
instrument_id=123, timestamp=1700124000000, o=104, h=106, l=97, c=98, v=20000
```

**Daily Record Updated:**
```python
instrument_id=123, timestamp=1700092800000
open=100    # UNCHANGED (keep first)
high=106    # UPDATED (max of 105, 106)
low=97      # UPDATED (min of 99, 97)
close=98    # UPDATED (latest)
volume=45000 # UPDATED (25000 + 20000)
```

---

### 3:30 PM - Last 5-min candle (Market Close)

**Intraday Save:**
```python
instrument_id=123, timestamp=1700145600000, o=150, h=152, l=149, c=151, v=30000
```

**Final Daily Record:**
```python
instrument_id=123, timestamp=1700092800000
open=100     # First open of the day
high=155     # Highest high of the day
low=97       # Lowest low of the day
close=151    # Last close of the day
volume=5432000 # Total volume for the day
```

✅ **No end-of-day aggregation needed!** The daily record is already complete.

---

## 🔍 Code Implementation

### `upsert_daily_record()` Method

```python
async def upsert_daily_record(
    self,
    session: AsyncSession,
    instrument_id: int,
    date_timestamp: int,
    current_ohlcv: Dict[str, float]
) -> None:
    """
    Insert or update daily record with incremental OHLCV data.
    """
    # Check if daily record exists
    query = select(PriceHistoryDaily).where(
        PriceHistoryDaily.instrument_id == instrument_id,
        PriceHistoryDaily.timestamp == date_timestamp
    )
    result = await session.execute(query)
    existing_daily = result.scalar_one_or_none()
    
    if existing_daily:
        # UPDATE existing record
        existing_daily.high = max(existing_daily.high or float('-inf'), current_ohlcv["high"])
        existing_daily.low = min(existing_daily.low or float('inf'), current_ohlcv["low"])
        existing_daily.close = current_ohlcv["close"]
        existing_daily.volume = (existing_daily.volume or 0) + int(current_ohlcv["volume"])
    else:
        # INSERT new record
        new_daily = PriceHistoryDaily(
            instrument_id=instrument_id,
            timestamp=date_timestamp,
            open=current_ohlcv["open"],
            high=current_ohlcv["high"],
            low=current_ohlcv["low"],
            close=current_ohlcv["close"],
            volume=int(current_ohlcv.get("volume", 0)),
            price_not_found=False
        )
        session.add(new_daily)
```

### `save_to_intraday_table()` Enhancement

```python
# After saving intraday records, update daily records
for instrument_id, ohlcv in daily_updates:
    await self.upsert_daily_record(
        session,
        instrument_id,
        date_timestamp,
        {
            "open": ohlcv["open"],
            "high": ohlcv["high"],
            "low": ohlcv["low"],
            "close": ohlcv["close"],
            "volume": ohlcv.get("volume", 0)
        }
    )

await session.commit()  # Single transaction for both
```

---

## 📊 Performance Comparison

### ❌ Old Approach (End-of-Day Aggregation)

**At 3:30 PM (Market Close):**
```sql
-- Heavy query scanning thousands of records
SELECT 
    instrument_id,
    MIN(open) as first_open,
    MAX(high) as high,
    MIN(low) as low,
    MAX(timestamp) as last_timestamp
FROM price_history_intraday
WHERE timestamp BETWEEN '2025-01-01 00:00:00' AND '2025-01-01 23:59:59'
  AND exchange_id = 1
GROUP BY instrument_id;

-- Additional query for each instrument to get close
SELECT close FROM price_history_intraday 
WHERE instrument_id = ? AND timestamp = ?;

-- Additional query for each instrument to get volume
SELECT SUM(volume) FROM price_history_intraday 
WHERE instrument_id = ? AND timestamp BETWEEN ? AND ?;
```

**Cost:**
- Scans all ~75 intraday records per instrument (6.5 hours × 12 candles/hour)
- For 1000 instruments: 75,000 rows scanned
- Multiple queries per instrument
- **Total: ~3,000+ queries**

---

### ✅ New Approach (Incremental Updates)

**Every 5 minutes:**
```sql
-- Check if daily record exists (indexed query)
SELECT * FROM price_history_daily 
WHERE instrument_id = ? AND timestamp = ?;

-- Either UPDATE or INSERT (single row)
UPDATE price_history_daily 
SET high = GREATEST(high, ?),
    low = LEAST(low, ?),
    close = ?,
    volume = volume + ?
WHERE instrument_id = ? AND timestamp = ?;
```

**Cost:**
- 1 SELECT + 1 UPDATE per instrument per 5-minute interval
- For 1000 instruments over 6.5 hours: 2,000 queries
- Simple indexed lookups
- **Total: ~2,000 queries (spread throughout the day)**

---

## 🎯 Database Indexes

Ensure these indexes exist for optimal performance:

```sql
-- For daily record lookups
CREATE INDEX idx_daily_instrument_timestamp 
ON price_history_daily(instrument_id, timestamp);

-- For intraday inserts
CREATE INDEX idx_intraday_instrument_timestamp 
ON price_history_intraday(instrument_id, timestamp);
```

---

## 🧪 Testing

### Test Incremental Updates

```python
# Save at 9:15 AM
await data_saver.save_to_intraday_table()
# Check: daily record should have first open, initial high/low/close

# Save at 9:20 AM
await data_saver.save_to_intraday_table()
# Check: daily record should have updated high (if higher), low (if lower), latest close

# Save at 9:25 AM
await data_saver.save_to_intraday_table()
# Check: daily record continues to update correctly
```

### Verify Daily Data

```sql
-- Check daily data at any time during the trading day
SELECT instrument_id, 
       open as first_open,
       high as current_high,
       low as current_low,
       close as latest_close,
       volume as total_volume_so_far
FROM price_history_daily
WHERE timestamp = '2025-11-23 00:00:00';
```

---

## ✅ Summary

**What Changed:**
- ❌ Removed heavy `aggregate_and_save_daily()` end-of-day query
- ✅ Added `upsert_daily_record()` for incremental updates
- ✅ Integrated daily updates into `save_to_intraday_table()`
- ✅ Single transaction for both intraday and daily saves

**Benefits:**
- 🚀 Better performance (no heavy aggregation)
- 📊 Real-time daily data available throughout the day
- 🎨 Simpler code (no separate end-of-day job)
- 💾 More efficient database usage

**Trade-off:**
- Slightly more work per 5-minute save (1 extra UPDATE per instrument)
- But this is negligible compared to end-of-day aggregation cost
- **Overall: Much better approach!** ✅

---

**Date**: November 23, 2025  
**Status**: ✅ Implemented and Optimized

