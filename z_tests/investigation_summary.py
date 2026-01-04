"""
OHLC Singular Value Investigation - Summary and Solution

This document summarizes the investigation into why OHLC values appear as dashes
(O=H=L=C) on charts for instruments like KOTAKBANK.

============================================================================
PROBLEM STATEMENT
============================================================================

For KOTAKBANK and other instruments, certain 5-minute candles in the database
had identical OHLC values (Open = High = Low = Close), appearing as flat lines
or dashes on charts. This is incorrect for liquid stocks where prices change
frequently during any 5-minute window.

============================================================================
INVESTIGATION FINDINGS
============================================================================

1. DATA QUALITY ISSUES DETECTED:
   - Before fix: 24 singular candles (O=H=L=C) and 32 NULL candles
   - Singular candles had very low volume (1-9) vs normal candles (100+)
   - This indicated only 1-9 ticks were received during those 5-min periods

2. ROOT CAUSE IDENTIFIED: WebSocket Connection Instability

   Evidence:
   - Problem periods were CLUSTERED, not random
   - A 12+ minute gap was found (13:17 to 13:29 IST)
   - Multiple instruments affected simultaneously
   - Connection gaps correlated with singular OHLC periods

   What happens:
   - WebSocket connection to Dhan drops temporarily
   - During disconnection, no ticks are received
   - When reconnected, only 1-2 ticks may be captured before bucket ends
   - Result: O=H=L=C (singular candle)

3. CONTRIBUTING FACTORS:

   a) Quote Mode (Code 17) limitations:
      - Only sends updates when a trade happens
      - No heartbeat during quiet periods
      - Cannot distinguish "no trades" from "disconnected"

   b) Redis TimeSeries 15-minute retention:
      - Cannot retroactively fill missing data
      - By the time gap is detected, tick data is gone

   c) No automatic backfill mechanism:
      - System had no way to recover from data gaps

============================================================================
SOLUTION IMPLEMENTED
============================================================================

1. BACKFILL SERVICE (gap_detector_backfill.py):
   - Detects gaps in database (NULL or singular candles with low volume)
   - Fetches correct OHLC data from Dhan REST API
   - Updates database records with accurate data
   - Successfully fixed 59+ records for KOTAKBANK
   - Supports batch backfill for ALL symbols: python z_tests/gap_detector_backfill.py --all

RESULTS AFTER BACKFILL:
   BEFORE: Normal=19, Singular=24, NULL=32
   AFTER:  Normal=60, Singular=1,  NULL=14 (future candles)

2. ANALYSIS TOOLS CREATED:
   - investigate_single_ohlc.py - Initial investigation script
   - dhan_data_quality_analysis.py - Deep data quality analysis
   - root_cause_analysis.py - Root cause determination
   - realtime_tick_monitor.py - Live tick rate monitoring
   - timestamp_investigation.py - Timestamp format analysis

============================================================================
RECOMMENDED ONGOING FIXES
============================================================================

IMMEDIATE (to prevent future issues):

1. Schedule periodic backfill:
   - Run backfill service every 15 minutes during market hours
   - Run end-of-day backfill to ensure complete data

2. Add connection monitoring:
   - Log WebSocket reconnection events with timestamps
   - Alert when connection drops detected

SHORT-TERM:

3. Implement heartbeat detection:
   - Track last tick time per symbol
   - Alert if no ticks received for > 60 seconds

4. Auto-mark affected records:
   - When reconnection detected, mark recent records for resolution

LONG-TERM:

5. Consider Full Mode (Code 21):
   - Provides more frequent updates (market depth)
   - More resilient to brief connection drops

6. Implement redundant data sources:
   - Secondary provider as backup
   - Cross-validate data between sources

============================================================================
USAGE
============================================================================

To run backfill for a specific symbol:

    python z_tests/gap_detector_backfill.py

To analyze data quality:

    python z_tests/dhan_data_quality_analysis.py

To monitor live tick rates:

    python z_tests/realtime_tick_monitor.py --symbols KOTAKBANK --duration 300

============================================================================
FILES CREATED
============================================================================

1. z_tests/investigate_single_ohlc.py
   - Initial investigation script with multiple test cases

2. z_tests/dhan_data_quality_analysis.py
   - Deep analysis of Redis and database data quality

3. z_tests/root_cause_analysis.py
   - Root cause determination and problem period analysis

4. z_tests/realtime_tick_monitor.py
   - Live monitoring of tick rates and connection stability

5. z_tests/timestamp_investigation.py
   - Investigation of timestamp format differences

6. z_tests/gap_detector_backfill.py
   - Solution: Detects gaps and backfills using Dhan REST API

7. z_tests/investigation_summary.py (this file)
   - Complete documentation of investigation and solution

============================================================================
"""

print(__doc__)

