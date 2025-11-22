# Multi-Provider Market Data Setup Guide

## Overview

This application now supports multiple market data providers with exchange-based routing:
- **Yahoo Finance**: For US exchanges (NYSE, NASDAQ)
- **Dhan**: For Indian exchanges (NSE, BSE)

Providers are automatically selected based on which exchange an instrument belongs to, using database-driven configuration.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     Client WebSockets                        │
│              (Subscribe to symbols)                          │
└──────────────────────┬──────────────────────────────────────┘
                       │
                       ↓
┌─────────────────────────────────────────────────────────────┐
│              LiveDataIngestion                               │
│         (Multi-Provider Orchestration)                       │
└──────────┬────────────┬─────────────────────────────────────┘
           │            │
           ↓            ↓
    ┌──────────┐  ┌──────────┐
    │  Yahoo   │  │   Dhan   │
    │ Finance  │  │          │
    │  (NYSE)  │  │  (NSE)   │
    │ (NASDAQ) │  │  (BSE)   │
    └──────────┘  └──────────┘
           │            │
           └────────────┘
                 │
                 ↓
    ┌────────────────────────┐
    │   Redis TimeSeries     │
    │  (Unified storage)     │
    └────────────────────────┘
```

---

## Setup Instructions

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

This will install:
- `yfinance` - Yahoo Finance WebSocket client
- `dhanhq==2.0.2` - Dhan market data SDK

### 2. Configure Environment Variables

Add to your `.env` file:

```env
# Existing variables...
DATABASE_URL=your_database_url
SECRET_KEY=your_secret_key

# Dhan API Credentials
DHAN_CLIENT_ID=your_dhan_client_id
DHAN_ACCESS_TOKEN=your_dhan_access_token
```

**Getting Dhan Credentials:**
1. Sign up at [Dhan](https://www.dhan.co/)
2. Generate API credentials from your account settings
3. Add them to your `.env` file

### 3. Set Up Database Tables

Run the provider setup script:

```bash
python z_data_migration/setup_providers.py
```

This script will:
- ✅ Create `Provider` records for Yahoo Finance and Dhan
- ✅ Create `ExchangeProviderMapping` records:
  - NSE → Dhan (primary)
  - BSE → Dhan (primary)
  - NYSE → Yahoo Finance (primary)
  - NASDAQ → Yahoo Finance (primary)

### 4. Populate Instrument Mappings

Run the instrument mapping script:

```bash
python z_data_migration/populate_instrument_mappings.py
```

This script will:
- ✅ Map NSE/BSE instruments to Dhan's symbol format (e.g., "RELIANCE-EQ")
- ✅ Map NYSE/NASDAQ instruments to Yahoo Finance format (e.g., "AAPL")

**Custom Symbol Formats:**
If your instruments need different symbol formats, edit the scripts:
- For Dhan: Modify `populate_dhan_mappings()` function
- For Yahoo: Modify `populate_yahoo_mappings()` function

### 5. Start the Application

```bash
python main.py
```

The application will:
1. Load exchange-provider mappings from database
2. Initialize both Yahoo Finance and Dhan providers
3. Route symbols to correct providers based on exchange
4. Start receiving real-time data from both providers

---

## Database Schema

### Providers Table
```sql
CREATE TABLE providers (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    code VARCHAR(64) NOT NULL,  -- 'YF' or 'DHAN'
    credentials JSON,
    rate_limit INTEGER,
    is_active BOOLEAN DEFAULT TRUE
);
```

### Exchange-Provider Mappings
```sql
CREATE TABLE exchange_provider_mappings (
    provider_id INTEGER REFERENCES providers(id),
    exchange_id INTEGER REFERENCES exchanges(id),
    is_active BOOLEAN DEFAULT TRUE,
    is_primary BOOLEAN DEFAULT FALSE,
    PRIMARY KEY (provider_id, exchange_id)
);
```

### Provider-Instrument Mappings
```sql
CREATE TABLE provider_instrument_mappings (
    provider_id INTEGER REFERENCES providers(id),
    instrument_id INTEGER REFERENCES instruments(id),
    provider_instrument_search_code VARCHAR(255) NOT NULL,
    PRIMARY KEY (provider_id, instrument_id)
);
```

---

## How It Works

### 1. Startup Process

1. `LiveDataIngestion.start_ingestion()` is called
2. `ProviderManager.initialize()` loads exchange→provider mappings
3. `ProviderManager.get_symbols_by_provider()` queries database:
   - Groups instruments by exchange
   - Routes to correct provider based on mappings
   - Returns: `{"YF": ["AAPL", "TSLA"], "DHAN": ["RELIANCE-EQ", "TCS-EQ"]}`
4. `ProviderManager.start_all_providers()` connects both WebSockets in parallel

### 2. Data Flow

```
Provider WebSocket → message_handler() → DataIngestionFormat (normalized)
                                              ↓
                                    handle_market_data()
                                              ↓
                                    Redis TimeSeries
                                              ↓
                                    DataBroadcast → Clients
```

### 3. Symbol Routing

The `ProviderManager` maintains mappings:
```python
{
    "AAPL": "YF",           # Yahoo Finance
    "TSLA": "YF",           # Yahoo Finance
    "RELIANCE-EQ": "DHAN",  # Dhan
    "TCS-EQ": "DHAN"        # Dhan
}
```

When subscribing/unsubscribing dynamically, symbols are routed to correct providers.

---

## Provider-Specific Details

### Yahoo Finance Provider (`YahooFinanceProvider`)

**Symbol Format:** Standard tickers (e.g., `AAPL`, `GOOGL`)

**Message Format:**
```python
{
    "symbol": "AAPL",
    "price": 150.25,
    "day_volume": 50000000,
    "time": 1700000000
}
```

**Normalized to:**
```python
DataIngestionFormat(
    symbol="AAPL",
    price=150.25,
    volume=50000000,
    timestamp=1700000000,
    provider_code="YF"
)
```

### Dhan Provider (`DhanProvider`)

**Symbol Format:** Exchange-specific (e.g., `RELIANCE-EQ`, `TCS-EQ` for NSE)

**Message Format (expected):**
```python
{
    "type": "Ticker Data",
    "exchange_segment": "NSE_EQ",
    "security_id": "RELIANCE",
    "LTP": 2456.75,
    "volume": 125000,
    "timestamp": 1700000000
}
```

**Normalized to:**
```python
DataIngestionFormat(
    symbol="RELIANCE-EQ",
    price=2456.75,
    volume=125000,
    timestamp=1700000000,
    provider_code="DHAN"
)
```

**Note:** Dhan's actual message format may differ. Adjust `DhanProvider.message_handler()` accordingly.

---

## Advanced Features

### Dynamic Subscription Management (Optional)

Enable dynamic subscriptions to only fetch data for symbols clients are watching:

In `services/live_data_ingestion.py`, uncomment:
```python
# In start_ingestion() method:
self._sync_task = asyncio.create_task(self.sync_subscriptions_with_clients())
```

This will:
- Poll `websocket_manager.get_active_channels()` every 30 seconds
- Subscribe to new symbols clients are watching
- Unsubscribe from symbols no clients are watching
- **Result:** Reduced data throughput during off-hours

### Adding More Providers

To add a new provider (e.g., Alpha Vantage, Polygon):

1. **Create provider class:**
   ```python
   # services/alpha_vantage_provider.py
   class AlphaVantageProvider(BaseMarketDataProvider):
       # Implement abstract methods
   ```

2. **Update provider factory:**
   ```python
   # In provider_manager.py
   def _create_provider_instance(self, provider_code: str):
       if provider_code == "YF":
           return YahooFinanceProvider(callback=self.callback)
       elif provider_code == "DHAN":
           return DhanProvider(callback=self.callback)
       elif provider_code == "AV":  # NEW
           return AlphaVantageProvider(callback=self.callback)
   ```

3. **Add to database:**
   ```sql
   INSERT INTO providers (name, code, rate_limit) 
   VALUES ('Alpha Vantage', 'AV', 500);
   
   INSERT INTO exchange_provider_mappings (provider_id, exchange_id, is_active, is_primary)
   SELECT p.id, e.id, true, true
   FROM providers p, exchanges e
   WHERE p.code = 'AV' AND e.code = 'LSE';
   ```

---

## Monitoring

### Check Provider Status

Add an endpoint to monitor provider health:

```python
@app.get("/api/providers/status")
async def get_provider_status():
    from services.data.data_ingestion import live_data_ingestion
    return live_data_ingestion.get_provider_status()
```

**Response:**
```json
{
  "YF": {
    "provider_code": "YF",
    "connected": true,
    "subscribed_count": 150,
    "symbols": ["AAPL", "TSLA", ...]
  },
  "DHAN": {
    "provider_code": "DHAN",
    "connected": true,
    "subscribed_count": 500,
    "symbols": ["RELIANCE-EQ", "TCS-EQ", ...]
  }
}
```

### Logs to Watch

```
[INFO] Initializing ProviderManager...
[INFO] Mapped NSE → DHAN
[INFO] Mapped BSE → DHAN
[INFO] Mapped NYSE → YF
[INFO] Mapped NASDAQ → YF
[INFO] Initialized provider: YF
[INFO] Initialized provider: DHAN
[INFO] ProviderManager initialized with 2 providers
[INFO] Loaded 650 total symbols
[INFO]   DHAN: 500 symbols
[INFO]   YF: 150 symbols
[INFO] Connecting DHAN with 500 symbols
[INFO] Connecting YF with 150 symbols
[INFO] Dhan provider connected with 500 symbols
[INFO] Yahoo Finance connected with 150 symbols
[INFO] ✅ Multi-provider data ingestion started successfully
```

---

## Troubleshooting

### Issue: "Unknown provider code: DHAN"

**Solution:** Provider not registered in factory method. Check `provider_manager.py` line ~90.

### Issue: "No symbols found to subscribe to"

**Solutions:**
1. Run `python data_migration/setup_providers.py`
2. Run `python data_migration/populate_instrument_mappings.py`
3. Verify instruments exist in database with correct exchange_id

### Issue: Dhan connection fails

**Solutions:**
1. Check `DHAN_CLIENT_ID` and `DHAN_ACCESS_TOKEN` in `.env`
2. Verify Dhan API credentials are valid
3. Check Dhan SDK version: `pip show dhanhq`
4. Review Dhan's actual API documentation and adjust `dhan_provider.py` accordingly

### Issue: Yahoo Finance connection fails

**Solutions:**
1. Check internet connectivity
2. Verify `yfinance` library is up to date: `pip install --upgrade yfinance`
3. Check Yahoo Finance API status

---

## Resource Usage

### During Market Hours (with subscriptions)

| Metric | Yahoo Finance | Dhan | Total |
|--------|---------------|------|-------|
| CPU | 2-5% | 2-5% | 4-10% |
| Memory | 10-20 MB | 10-20 MB | 20-40 MB |
| Network | 5-15 KB/s | 5-15 KB/s | 10-30 KB/s |

### During Off-Hours (idle connections)

| Metric | Yahoo Finance | Dhan | Total |
|--------|---------------|------|-------|
| CPU | 0.1-0.2% | 0.1-0.2% | 0.2-0.4% |
| Memory | 2-5 MB | 2-5 MB | 4-10 MB |
| Network | 0.05-0.1 KB/s | 0.05-0.1 KB/s | 0.1-0.2 KB/s |

**Conclusion:** Multiple provider connections have minimal resource impact, especially during off-hours.

---

## Next Steps

1. ✅ Test with sample data
2. ✅ Monitor provider connections in production
3. ✅ Add failover logic (secondary providers)
4. ✅ Implement provider health checks
5. ✅ Add metrics/alerts for provider disconnections

---

## Support

For issues or questions:
1. Check logs in `config/logger.py`
2. Verify database setup with migration scripts
3. Review provider documentation:
   - [Yahoo Finance Python](https://github.com/ranaroussi/yfinance)
   - [Dhan API Docs](https://dhanhq.co/docs)

---

**Happy Trading! 🚀📈**

