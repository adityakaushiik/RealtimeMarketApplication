# DataSaver - Market Data Persistence Service

## Overview

The DataSaver service handles periodic collection and persistence of market data from Redis TimeSeries to PostgreSQL database. It supports multiple exchanges with different trading hours and automatically aggregates intraday data to daily summaries.

## Key Features

✅ **Periodic 5-minute Saves**: Automatically saves OHLCV data from Redis to `price_history_intraday` table  
✅ **Daily Aggregation**: Aggregates intraday data to `price_history_daily` table at end of trading day  
✅ **Multi-Exchange Support**: Handle multiple exchanges with different trading hours simultaneously  
✅ **Timezone Aware**: Works with millisecond timestamps for precise time handling  
✅ **Graceful Shutdown**: Proper cleanup and final saves when stopping  
✅ **Error Handling**: Robust error handling with detailed logging  

---

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                        DataSaver                             │
├─────────────────────────────────────────────────────────────┤
│  + add_exchange(ExchangeData)                               │
│  + start_all_exchanges()                                     │
│  + stop_all_exchanges()                                      │
│  + save_to_intraday_table()                                  │
│  + aggregate_and_save_daily()                                │
└─────────────────────────────────────────────────────────────┘
                          │
                          ├── Uses ──────────────┐
                          │                      │
                          ▼                      ▼
            ┌──────────────────────┐   ┌──────────────────┐
            │   ExchangeData       │   │ RedisTimeSeries  │
            ├──────────────────────┤   ├──────────────────┤
            │ - exchange_name      │   │ - get_all_keys() │
            │ - exchange_id        │   │ - get_ohlcv_*()  │
            │ - start_time (ms)    │   └──────────────────┘
            │ - end_time (ms)      │
            │ - interval_minutes   │
            └──────────────────────┘
                          │
                          │ Saves to
                          ▼
            ┌──────────────────────────────────┐
            │         Database                 │
            ├──────────────────────────────────┤
            │  price_history_intraday          │
            │  ├─ instrument_id                │
            │  ├─ timestamp                    │
            │  ├─ open, high, low, close       │
            │  ├─ volume                       │
            │  └─ interval (5m)                │
            │                                  │
            │  price_history_daily             │
            │  ├─ instrument_id                │
            │  ├─ timestamp (start of day)     │
            │  ├─ open, high, low, close       │
            │  └─ volume (sum)                 │
            └──────────────────────────────────┘
```

---

## Quick Start

### 1. Create ExchangeData Configuration

```python
from datetime import datetime
from services.data.exchange_data import ExchangeData

# Define market hours (e.g., NSE: 9:15 AM - 3:30 PM IST)
today = datetime.now()
start_time = today.replace(hour=9, minute=15, second=0, microsecond=0)
end_time = today.replace(hour=15, minute=30, second=0, microsecond=0)

# Convert to milliseconds
start_time_ms = int(start_time.timestamp() * 1000)
end_time_ms = int(end_time.timestamp() * 1000)

# Create exchange configuration
nse_exchange = ExchangeData(
    exchange_name="NSE",
    exchange_id=1,  # Your database exchange ID
    start_time=start_time_ms,
    end_time=end_time_ms,
    interval_minutes=5,  # Save every 5 minutes
)
```

### 2. Initialize and Run DataSaver

```python
from services.data.data_saver import DataSaver

# Initialize DataSaver
data_saver = DataSaver()

# Add exchange(s)
data_saver.add_exchange(nse_exchange)

# Start periodic saves
await data_saver.start_all_exchanges()

# Wait for completion
await data_saver.wait_for_completion()
```

---

## Detailed Usage

### ExchangeData Class

**Purpose**: Encapsulates configuration for a single exchange's data collection schedule.

**Constructor Parameters**:
- `exchange_name` (str): Display name (e.g., "NSE", "BSE")
- `exchange_id` (int): Database ID from `exchanges` table
- `start_time` (int): Market open time in milliseconds
- `end_time` (int): Market close time in milliseconds
- `interval_minutes` (int): How often to save data (default: 5)
- `api_key` (Optional[str]): API credentials if needed
- `api_secret` (Optional[str]): API credentials if needed

**Methods**:
```python
# Get readable exchange info
info = exchange_data.get_exchange_info()
# Returns: dict with readable timestamps and configuration

# Check if market is currently open
is_open = exchange_data.is_market_open(current_time_ms)

# Get remaining time until market close
remaining_ms = exchange_data.get_remaining_time_ms(current_time_ms)
```

### DataSaver Class

#### Constructor
```python
data_saver = DataSaver()
```
No parameters needed. Automatically initializes Redis TimeSeries connection.

#### Methods

**1. add_exchange(exchange_data: ExchangeData) -> None**

Add an exchange to monitor for data collection.

```python
data_saver.add_exchange(nse_exchange)
data_saver.add_exchange(bse_exchange)
```

**2. start_all_exchanges() -> None**

Start periodic saves for all registered exchanges. Creates async tasks for each exchange.

```python
await data_saver.start_all_exchanges()
```

**3. save_to_intraday_table(exchange_id: Optional[int], instrument_ids: Optional[List[int]]) -> int**

Manually save current 5-minute OHLCV data from Redis to database.

```python
# Save all exchanges
count = await data_saver.save_to_intraday_table()

# Save specific exchange only
count = await data_saver.save_to_intraday_table(exchange_id=1)

# Save specific instruments only
count = await data_saver.save_to_intraday_table(
    exchange_id=1, 
    instrument_ids=[10, 20, 30]
)
```

**4. aggregate_and_save_daily(exchange_data: ExchangeData, date_timestamp: int) -> int**

Aggregate intraday data for a specific date and save to daily table.

```python
from datetime import datetime

# Get today's date (midnight)
today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
date_timestamp = int(today.timestamp() * 1000)

# Aggregate
count = await data_saver.aggregate_and_save_daily(nse_exchange, date_timestamp)
```

**5. stop_all_exchanges() -> None**

Stop all running periodic saves gracefully.

```python
await data_saver.stop_all_exchanges()
```

**6. wait_for_completion() -> None**

Wait for all periodic save tasks to complete naturally (when end_time is reached).

```python
await data_saver.wait_for_completion()
```

---

## Data Flow

### Intraday Save Process

```
Every 5 minutes during market hours:

1. Redis TimeSeries:
   └─ get_all_keys() → ["AAPL", "GOOGL", ...]
   └─ get_all_ohlcv_last_5m(keys) → {symbol: {ts, o, h, l, c, v}}

2. Database Query:
   └─ Map symbols to instrument_ids
   └─ Filter by exchange_id, blacklisted, delisted

3. Save to price_history_intraday:
   └─ instrument_id
   └─ timestamp (aligned to 5-min slot)
   └─ open, high, low, close
   └─ volume
   └─ interval = "5m"
```

### Daily Aggregation Process

```
At end_time (market close):

1. Query price_history_intraday:
   └─ WHERE timestamp BETWEEN day_start AND day_end
   └─ GROUP BY instrument_id
   └─ Aggregate:
      - open = first open of day
      - high = max(high)
      - low = min(low)
      - close = last close of day
      - volume = sum(volume)

2. Save to price_history_daily:
   └─ instrument_id
   └─ timestamp = start_of_day (midnight)
   └─ open, high, low, close
   └─ volume (total for day)
```

---

## Example Scenarios

### Scenario 1: Single Exchange (Production)

```python
import asyncio
from datetime import datetime
from services.data.data_saver import DataSaver
from services.data.exchange_data import ExchangeData


async def run_nse_collection():
    # NSE trading hours: 9:15 AM - 3:30 PM IST
    today = datetime.now()
    start = today.replace(hour=9, minute=15, second=0, microsecond=0)
    end = today.replace(hour=15, minute=30, second=0, microsecond=0)

    nse = ExchangeData(
        exchange_name="NSE",
        exchange_id=1,
        start_time=int(start.timestamp() * 1000),
        end_time=int(end.timestamp() * 1000),
        interval_minutes=5,
    )

    data_saver = DataSaver()
    data_saver.add_exchange(nse)

    await data_saver.start_all_exchanges()
    await data_saver.wait_for_completion()


asyncio.run(run_nse_collection())
```

### Scenario 2: Multiple Exchanges

```python
async def run_multiple_exchanges():
    today = datetime.now()
    
    # NSE: 9:15 AM - 3:30 PM
    nse = ExchangeData(
        exchange_name="NSE",
        exchange_id=1,
        start_time=int(today.replace(hour=9, minute=15).timestamp() * 1000),
        end_time=int(today.replace(hour=15, minute=30).timestamp() * 1000),
        interval_minutes=5,
    )
    
    # BSE: 9:15 AM - 3:30 PM (same as NSE)
    bse = ExchangeData(
        exchange_name="BSE",
        exchange_id=2,
        start_time=int(today.replace(hour=9, minute=15).timestamp() * 1000),
        end_time=int(today.replace(hour=15, minute=30).timestamp() * 1000),
        interval_minutes=5,
    )
    
    data_saver = DataSaver()
    data_saver.add_exchange(nse)
    data_saver.add_exchange(bse)
    
    await data_saver.start_all_exchanges()
    await data_saver.wait_for_completion()

asyncio.run(run_multiple_exchanges())
```

### Scenario 3: Testing with Short Duration

```python
import time

async def test_data_saver():
    # Start in 1 minute, run for 10 minutes
    start = int((time.time() + 60) * 1000)
    end = int((time.time() + 600) * 1000)
    
    test_exchange = ExchangeData(
        exchange_name="TEST",
        exchange_id=1,
        start_time=start,
        end_time=end,
        interval_minutes=1,  # Save every minute for testing
    )
    
    data_saver = DataSaver()
    data_saver.add_exchange(test_exchange)
    
    await data_saver.start_all_exchanges()
    await data_saver.wait_for_completion()

asyncio.run(test_data_saver())
```

### Scenario 4: Manual End-of-Day Aggregation

```python
async def manual_daily_aggregation():
    """Run this as a scheduled job at end of day"""
    from datetime import datetime
    
    data_saver = DataSaver()
    
    # Configure exchange (times not important for manual aggregation)
    nse = ExchangeData(
        exchange_name="NSE",
        exchange_id=1,
        start_time=0,
        end_time=0,
    )
    
    # Get today's date
    today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    date_ts = int(today.timestamp() * 1000)
    
    # Aggregate
    count = await data_saver.aggregate_and_save_daily(nse, date_ts)
    print(f"Aggregated {count} daily records")

asyncio.run(manual_daily_aggregation())
```

---

## Integration with FastAPI

### Startup/Shutdown Events

```python
from fastapi import FastAPI
from services.data.data_saver import DataSaver
from services.data.exchange_data import ExchangeData

app = FastAPI()
data_saver = DataSaver()


@app.on_event("startup")
async def startup_event():
    # Configure exchanges
    nse = ExchangeData(
        exchange_name="NSE",
        exchange_id=1,
        start_time=get_market_open_time(),
        end_time=get_market_close_time(),
        interval_minutes=5,
    )

    data_saver.add_exchange(nse)
    await data_saver.start_all_exchanges()


@app.on_event("shutdown")
async def shutdown_event():
    await data_saver.stop_all_exchanges()
```

### Background Tasks

```python
from fastapi import BackgroundTasks

@app.post("/start-data-collection")
async def start_collection(background_tasks: BackgroundTasks):
    background_tasks.add_task(data_saver.start_all_exchanges)
    return {"status": "started"}

@app.post("/save-current-data")
async def save_current():
    count = await data_saver.save_to_intraday_table()
    return {"saved": count}
```

---

## Database Schema

### price_history_intraday

| Column | Type | Description |
|--------|------|-------------|
| id | INTEGER | Primary key |
| instrument_id | INTEGER | FK to instruments table |
| timestamp | BIGINT | Unix timestamp in milliseconds |
| open | FLOAT | Opening price |
| high | FLOAT | Highest price |
| low | FLOAT | Lowest price |
| close | FLOAT | Closing price |
| volume | INTEGER | Trading volume |
| interval | VARCHAR(32) | "5m" for 5-minute |
| price_not_found | BOOLEAN | Error flag |

### price_history_daily

| Column | Type | Description |
|--------|------|-------------|
| id | INTEGER | Primary key |
| instrument_id | INTEGER | FK to instruments table |
| timestamp | BIGINT | Start of day in milliseconds |
| open | FLOAT | First open of day |
| high | FLOAT | Highest price of day |
| low | FLOAT | Lowest price of day |
| close | FLOAT | Last close of day |
| volume | INTEGER | Total volume for day |
| price_not_found | BOOLEAN | Error flag |

---

## Logging

The DataSaver uses Python's standard logging module. Configure log levels:

```python
import logging

# Set to DEBUG for verbose output
logging.getLogger("services.data.data_saver").setLevel(logging.DEBUG)

# Key log messages:
# - INFO: Exchange added, periodic save started/completed
# - WARNING: No data found, no keys in Redis
# - ERROR: Database errors, save failures
```

---

## Error Handling

### Graceful Degradation

- **Missing instruments**: Skipped with debug log
- **Incomplete OHLCV**: Skipped with debug log  
- **Database errors**: Rolled back, logged, re-raised
- **Redis errors**: Handled by RedisTimeSeries layer

### Recovery

```python
try:
    await data_saver.start_all_exchanges()
except Exception as e:
    logger.error(f"Failed to start: {e}")
    # Cleanup and retry
    await data_saver.stop_all_exchanges()
    raise
```

---

## Performance Considerations

1. **Batch Inserts**: Uses `session.add_all()` for efficient bulk inserts
2. **Parallel Queries**: Redis queries use `asyncio.gather()` for concurrency
3. **Indexed Columns**: Ensure `instrument_id` and `timestamp` are indexed
4. **Connection Pooling**: Uses SQLAlchemy async connection pool

### Optimization Tips

```python
# For large datasets, consider filtering:
count = await data_saver.save_to_intraday_table(
    exchange_id=1,
    instrument_ids=top_100_instruments  # Only save top instruments
)
```

---

## Troubleshooting

### Issue: No data saved

**Check**:
1. Redis TimeSeries has data: `await redis_ts.get_all_keys()`
2. Instruments exist in database
3. Instruments not blacklisted/delisted
4. Exchange ID matches

### Issue: Daily aggregation returns 0

**Check**:
1. Intraday data exists for the date
2. Date timestamp is start of day (midnight)
3. Exchange ID matches intraday records
4. price_not_found = False

### Issue: Timestamp alignment issues

**Verify**:
- All timestamps in milliseconds
- Use `int()` casting for timestamp values
- Check timezone consistency

---

## Best Practices

1. ✅ **Always use millisecond timestamps**
2. ✅ **Set appropriate interval_minutes** (5 min recommended)
3. ✅ **Handle timezones consistently** (use UTC internally)
4. ✅ **Monitor logs** for warnings/errors
5. ✅ **Test with short durations** before production
6. ✅ **Use graceful shutdown** with `stop_all_exchanges()`
7. ✅ **Schedule daily aggregation** as separate job if needed

---

## API Reference

See inline docstrings in:
- `services/data/data_saver.py`
- `services/data/ExchangeData.py`

For Redis TimeSeries methods:
- `services/redis_timeseries.py`

---

## Support

For issues or questions:
1. Check logs for error messages
2. Verify database schema matches models
3. Ensure Redis TimeSeries is running
4. Review example usage in `example_usage.py`

