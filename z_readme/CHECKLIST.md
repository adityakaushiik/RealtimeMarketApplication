# Multi-Provider Implementation Checklist

Use this checklist to ensure everything is set up correctly.

## ✅ Installation & Setup

- [ ] **Install dependencies**
  ```bash
  pip install -r requirements.txt
  ```
  - Installs `dhanhq==2.0.2` and other required packages

- [ ] **Configure environment variables**
  - Add to `.env` file:
    ```env
    DHAN_CLIENT_ID=your_dhan_client_id
    DHAN_ACCESS_TOKEN=your_dhan_access_token
    ```

- [ ] **Run provider setup script**
  ```bash
  python z_data_migration/setup_providers.py
  ```
  - Creates provider records (Yahoo Finance, Dhan)
  - Creates exchange-provider mappings

- [ ] **Run instrument mapping script**
  ```bash
  python z_data_migration/populate_instrument_mappings.py
  ```
  - Maps instruments to provider-specific symbols

- [ ] **Run verification script**
  ```bash
  python verify_setup.py
  ```
  - Checks all configuration
  - Should show "ALL CHECKS PASSED"

---

## ✅ Database Verification

- [ ] **Verify Providers table**
  ```sql
  SELECT id, name, code, is_active FROM providers;
  ```
  - Should show: Yahoo Finance (YF), Dhan (DHAN)

- [ ] **Verify Exchange-Provider Mappings**
  ```sql
  SELECT e.code as exchange, p.code as provider, epm.is_primary, epm.is_active
  FROM exchange_provider_mappings epm
  JOIN exchanges e ON e.id = epm.exchange_id
  JOIN providers p ON p.id = epm.provider_id
  WHERE epm.is_active = true;
  ```
  - NSE → DHAN (primary)
  - BSE → DHAN (primary)
  - NYSE → YF (primary)
  - NASDAQ → YF (primary)

- [ ] **Verify Instrument Mappings**
  ```sql
  SELECT COUNT(*) as count, p.code as provider
  FROM provider_instrument_mappings pim
  JOIN providers p ON p.id = pim.provider_id
  GROUP BY p.code;
  ```
  - Should show counts for both YF and DHAN

---

## ✅ Code Review

- [ ] **Review `services/base_provider.py`**
  - Abstract base class exists
  - All required methods defined

- [ ] **Review `services/yahoo_finance_connection.py`**
  - Inherits from `BaseMarketDataProvider`
  - Has subscribe/unsubscribe methods

- [ ] **Review `services/dhan_provider.py`**
  - Inherits from `BaseMarketDataProvider`
  - Message handler normalizes to `DataIngestionFormat`
  - **TODO: Adjust message format based on real Dhan API**

- [ ] **Review `services/provider_manager.py`**
  - Factory method includes both YF and DHAN
  - Routes symbols by exchange
  - Loads config from database

- [ ] **Review `services/live_data_ingestion.py`**
  - Uses `ProviderManager` instead of single provider
  - Callback passes `provider_code`

---

## ✅ Testing

- [ ] **Start application**
  ```bash
  python main.py
  ```

- [ ] **Check startup logs**
  - [ ] "Initializing ProviderManager..."
  - [ ] "Mapped NSE → DHAN"
  - [ ] "Mapped BSE → DHAN"
  - [ ] "Mapped NYSE → YF"
  - [ ] "Mapped NASDAQ → YF"
  - [ ] "Initialized provider: YF"
  - [ ] "Initialized provider: DHAN"
  - [ ] "Loaded X total symbols"
  - [ ] "Connecting DHAN with X symbols"
  - [ ] "Connecting YF with X symbols"
  - [ ] "✅ Multi-provider data ingestion started successfully"

- [ ] **Verify connections**
  - Check logs for successful connections
  - No error messages about providers

- [ ] **Test data flow**
  - Check Redis for incoming data
  - Verify data from both providers

---

## ✅ Dhan Provider Adjustments (Required)

- [ ] **Get actual Dhan WebSocket message format**
  - Review Dhan API documentation
  - Get sample WebSocket message

- [ ] **Update `DhanProvider.message_handler()`**
  - Adjust field names to match actual Dhan API
  - Test normalization to `DataIngestionFormat`

- [ ] **Update `DhanProvider._prepare_instruments()`**
  - Adjust symbol format parsing
  - Match your database symbol format

- [ ] **Test with real Dhan credentials**
  - Connect to Dhan WebSocket
  - Verify data is received
  - Check data quality

---

## ✅ Optional Enhancements

- [ ] **Enable dynamic subscriptions**
  - Uncomment in `live_data_ingestion.py`:
    ```python
    self._sync_task = asyncio.create_task(self.sync_subscriptions_with_clients())
    ```
  - Test subscribe/unsubscribe behavior

- [ ] **Add monitoring endpoint**
  - Create API endpoint:
    ```python
    @app.get("/api/providers/status")
    async def get_provider_status():
        return live_data_ingestion.get_provider_status()
    ```

- [ ] **Add health checks**
  - Monitor connection status
  - Alert on disconnections
  - Auto-reconnect logic

- [ ] **Add metrics**
  - Messages per second per provider
  - Connection uptime
  - Error rates

---

## ✅ Production Readiness

- [ ] **Load testing**
  - Test with full symbol list
  - Monitor resource usage
  - Check for memory leaks

- [ ] **Error handling**
  - Test provider disconnections
  - Verify reconnection logic
  - Check error logging

- [ ] **Documentation**
  - Team trained on new architecture
  - Runbook for provider issues
  - Monitoring alerts configured

- [ ] **Backup strategy**
  - Consider adding backup providers
  - Set `is_primary=false` for backups
  - Implement failover logic

---

## ✅ Go-Live Checklist

- [ ] All setup scripts run successfully
- [ ] All verification checks pass
- [ ] Dhan provider adjusted for real API
- [ ] Tested with real credentials
- [ ] Data flowing from both providers
- [ ] Monitoring in place
- [ ] Team trained
- [ ] Rollback plan ready

---

## 📋 Common Issues & Solutions

### Issue: "No providers found"
**Solution:** Run `python data_migration/setup_providers.py`

### Issue: "No instrument mappings"
**Solution:** Run `python data_migration/populate_instrument_mappings.py`

### Issue: "DHAN_CLIENT_ID not set"
**Solution:** Add credentials to `.env` file

### Issue: "Unknown provider code: DHAN"
**Solution:** Check `provider_manager.py` factory method includes DHAN

### Issue: "Dhan connection fails"
**Solution:** 
1. Verify credentials in `.env`
2. Check Dhan API status
3. Review `dhan_provider.py` message format

---

## 📞 Support Resources

- **Setup Guide:** `MULTI_PROVIDER_SETUP.md`
- **Implementation Details:** `IMPLEMENTATION_SUMMARY.md`
- **Verification Script:** `python verify_setup.py`
- **Logs:** Check application logs for detailed errors

---

## ✅ Sign-Off

- [ ] Setup complete
- [ ] All tests passing
- [ ] Production ready
- [ ] Documentation reviewed
- [ ] Team trained

**Date:** ________________

**Completed by:** ________________

**Approved by:** ________________

---

**Status:** 🟢 Ready for Production | 🟡 Testing | 🔴 Not Ready

