# Application Data Flow Documentation

This document describes the end-to-end data flow of the Realtime Market Application, covering data ingestion, processing, saving, resolution, and caching.

---

## 1. Provider Connection & Initial Mapping
**Goal:** Establish connection with market data providers (e.g., Dhan) and map external symbols to internal identifiers.

1.  **Provider Manager (`ProviderManager`)**:
    *   Initializes provider instances (e.g., `DhanProvider`).
    *   Loads all active instruments from the database.
    *   Group symbols by provider.
    *   Calls `connect_websocket(symbols)` on each provider.

2.  **Provider Mapping (`RedisMappingHelper`)**:
    *   **Pre-Loading:** Mappings between Internal Symbols (e.g., "RELIANCE") and Provider Symbols (e.g., "12345") are cached in Redis Hash Maps.
    *   **Segment Mapping:** Segments (e.g., "NSE_EQ", "NSE_FNO") are also cached in Redis to allow O(1) lookups during subscription.
    *   **Flow:** `Internal Symbol` -> `Redis Lookup` -> `Provider Symbol & Segment`.

---

## 2. Real-Time Data Ingestion Flow
**Goal:** Ingest live ticks, optimize them, and broadcast/store them with minimal latency.

### A. Data Receipt
1.  **Provider (`DhanProvider`)**: receives raw binary/JSON packets from the WebSocket.
2.  **Conversion**: Parses raw bytes into a standardized `DataIngestionFormat` object (Symbol, Price, Volume, Timestamp).
3.  **Queueing**: Pushes the object into the memory-buffer queue (`DataIngestionQueue`).

### B. Ingestion Processing (`LiveDataIngestion`)
The `LiveDataIngestion` worker consumes the queue effectively:

1.  **Volume Delta Calculation**:
    *   Providers often send "Total Cumulative Volume".
    *   The system calculates `Current Vol - Last Vol = Delta Vol` (the actual volume traded in this tick) for accurate candle aggregation.
2.  **Redis TimeSeries Storage**:
    *   **Key**: `realtime:{symbol}:5m`
    *   **Action**: `TS.ADD` (Adds the tick to the Redis TimeSeries with a retention policy).
    *   **Aggregation**: Redis automatically handles the OHLC (Open, High, Low, Close) aggregation for the 5-minute bucket in real-time.
3.  **Broadcasting**:
    *   Encodes the tick data into a compact binary format.
    *   Publishes to Redis Pub/Sub channel `market_data`.
    *   **WebSocket Manager**: Subscribes to `market_data` and pushes updates to connected frontend clients.

---

## 3. Data Persistence (Redis -> Database)
**Goal:** Persist high-speed cache data into permanent storage (PostgreSQL/TimescaleDB) periodically.

### The "Hourly Batch" Strategy (`DataSaver`)
Instead of writing to the DB on every tick, the system buffers data in Redis and flushes it hourly.

1.  **Scheduler**: `start_all_exchanges(interval_minutes=60)` triggers the loop.
2.  **Wait Loop**: The saver sleeps, aligning itself to the top of the hour (e.g., waits until 10:00:05).
3.  **Fetch**:
    *   Queries Redis TimeSeries for all 5-minute candles generated in the last hour.
4.  **Transformation**:
    *   Converts Redis output into `PriceHistoryIntraday` models.
5.  **Upsert (Insert/Update)**:
    *   Writes to the `price_history_intraday` table.
    *   Uses a "Check Existence first" or "On Conflict Update" pattern to strictly duplicate records.
    *   Updates `PriceHistoryDaily` (Day's Open, High, Low, Close) concurrently.
6.  **Checkpoint**: Updates `last_save_time:60m` in Redis to mark the timestamp of the last successful save.

---

## 4. Gap Detection & Resolution (The Safety Net)
**Goal:** Ensure 100% data completeness by catching missing candles caused by crashes or network failures.

### A. The "Buffer Zone" Strategy (`DataResolver`)
A background job runs hourly (`scheduled_jobs.py` -> `check_and_fill_gaps`).

1.  **Identify Gaps**:
    *   Scans the database for specific error conditions:
        *   `resolve_required = True`
        *   Missing/Null Price Data
    *   **Critial Logic**: It looks for errors **older than 60 minutes**.
    *   *Why?* To avoid racing with the `DataSaver`. If data is currently in Redis waiting to be saved, the Resolver ignores it.
2.  **Resolution Loop**:
    *   If gaps are found, it identifies the start/end time of the missing range.
    *   Calls `Provider.get_intraday_prices(start, end)`.
    *   Fills the missing rows in the Database.
    *   Marks `resolve_required = False`.

---

## 5. Reconnection & Recovery
**Goal:** Handle internet disconnects without losing real-time visibility.

1.  **Provider Reconnects**:
    *   `DhanProvider` detects connection loss and reconnects automatically.
    *   Resubscribes to all symbols.
2.  **Backfill (Redis-First)** (Conceptual Flow):
    *   Upon reconnection, the provider may fetch the last few minutes of missing data.
    *   This data is pushed to **Redis** (via `LiveDataIngestion`) to patch the live charts immediately.
    *   The `DataSaver` will later pick this up and write to DB.
    *   If any holes remain, the `DataResolver` (Step 4) will eventually catch and fix them.

---

## Summary Diagram

```mermaid
graph TD
    Provider[Provider WebSocket] -->|Raw Data| Connection[Provider Connection]
    Connection -->|Standardized Object| Queue[Data Ingestion Queue]
    
    subgraph "Real-Time Layer"
        Queue -->|Consume| Ingest[Live Data Ingestion]
        Ingest -->|TS.ADD| RedisTS[(Redis TimeSeries)]
        Ingest -->|Binary Pub| RedisPub[Redis Pub/Sub]
        RedisPub -->|Sub| WSMan[WebSocket Manager]
        WSMan -->|Push| Client[Frontend Client]
    end

    subgraph "Persistence Layer (Hourly)"
        Saver[Data Saver Service] -->|Wake Up (1h)| RedisTS
        RedisTS -->|Fetch Candles| Saver
        Saver -->|Upsert| DB[(PostgreSQL / TimescaleDB)]
    end

    subgraph "Audit Layer (Safety Net)"
        Resolver[Data Resolver Job] -->|Scan (Older than 1h)| DB
        DB -->|Found Gaps| Resolver
        Resolver -->|Fetch API| ProviderAPI[Provider REST API]
        ProviderAPI -->|Fill Gaps| DB
    end
```

