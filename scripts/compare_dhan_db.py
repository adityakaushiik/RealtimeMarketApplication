import asyncio
import os
import sys
from datetime import datetime, time, timezone, timedelta
import pandas as pd
import pytz
from sqlalchemy import select

# Add project root to path
# We are in scripts/, so parent is root
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.database_config import get_db_session
from config.logger import logger
from models import Instrument, PriceHistoryIntraday
from services.provider.dhan_provider import DhanProvider

# --- Configuration ---
# You can change these variables
SYMBOL_TO_CHECK = "HDFCBANK"
DATE_TO_CHECK_STR = "2026-01-16"  # Format: YYYY-MM-DD
# ---------------------


async def get_instrument_and_data_from_db(
    symbol: str, target_date_naive: datetime
) -> tuple[Instrument | None, pd.DataFrame | None]:
    """
    Fetches instrument details and intraday data from the database for the given date.
    """
    async for session in get_db_session():
        # Get instrument
        stmt = select(Instrument).where(Instrument.symbol == symbol)
        result = await session.execute(stmt)
        instrument = result.scalars().first()

        if not instrument:
            logger.error(f"Instrument {symbol} not found in DB")
            return None, pd.DataFrame()

        logger.info(f"Checking data for {instrument.symbol} (ID: {instrument.id})")

        # Calculate UTC range for the given IST date
        ist = pytz.timezone("Asia/Kolkata")

        # Start of day in IST
        start_of_day_ist = ist.localize(datetime.combine(target_date_naive, time.min))
        # End of day in IST
        end_of_day_ist = ist.localize(datetime.combine(target_date_naive, time.max))

        # Convert to UTC for DB query
        start_of_day_utc = start_of_day_ist.astimezone(timezone.utc)
        end_of_day_utc = end_of_day_ist.astimezone(timezone.utc)

        logger.info(
            f"Querying DB for UTC range (derived from IST date): {start_of_day_utc} to {end_of_day_utc}"
        )

        # Get intraday data
        stmt = (
            select(PriceHistoryIntraday)
            .where(
                PriceHistoryIntraday.instrument_id == instrument.id,
                PriceHistoryIntraday.datetime >= start_of_day_utc,
                PriceHistoryIntraday.datetime <= end_of_day_utc,
            )
            .order_by(PriceHistoryIntraday.datetime)
        )

        result = await session.execute(stmt)
        records = result.scalars().all()

        if not records:
            logger.warning("No records found in DB for this date")
            return instrument, pd.DataFrame()

        data = []
        for r in records:
            # Normalized to IST for comparison with Dhan (which thinks in IST but returns Epoch)
            # We want to match timestamps.
            # Dhan returns "Epoch timestamp".
            # If we store UTC in DB, we can convert to Epoch.

            # Use UTC timestamp for matching to be safe
            ts_utc = (
                r.datetime.replace(tzinfo=timezone.utc)
                if r.datetime.tzinfo is None
                else r.datetime
            )
            ts_epoch = int(ts_utc.timestamp())

            # Also keep readable IST time
            ts_ist = ts_utc.astimezone(ist)

            # Calculate "IST-as-UTC" epoch (Shifted Epoch)
            # This handles the case where Dhan returns timestamps that correspond to IST time but marked as UTC (or just raw seconds)
            # Example: 09:15 IST is 03:45 UTC.
            # Real Epoch of 09:15 IST = Epoch(03:45 UTC).
            # Shifted Epoch of 09:15 IST = Epoch(09:15 UTC).
            # Difference is 5.5 hours (19800 seconds).
            dt_naive_ist = ts_ist.replace(tzinfo=None)
            ts_shifted_epoch = int(
                dt_naive_ist.replace(tzinfo=timezone.utc).timestamp()
            )

            data.append(
                {
                    "timestamp_epoch": ts_epoch,
                    "timestamp_shifted": ts_shifted_epoch,
                    "timestamp_ist": ts_ist,
                    "open_db": r.open,
                    "high_db": r.high,
                    "low_db": r.low,
                    "close_db": r.close,
                    "volume_db": r.volume,
                }
            )

        df = pd.DataFrame(data)
        if not df.empty:
            df.set_index("timestamp_epoch", inplace=True)

        return instrument, df


async def get_dhan_data(
    instrument: Instrument, target_date_naive: datetime
) -> pd.DataFrame:
    """
    Fetches intraday data from Dhan provider.
    Replicates the logic inside DhanProvider.get_intraday_prices but returns raw DataFrame for comparison.
    """
    # Instantiate provider (without manager for direct access)
    provider = DhanProvider(provider_manager=None)

    # Calculate IST range
    ist_tz = pytz.timezone("Asia/Kolkata")
    start_date_ist = ist_tz.localize(datetime.combine(target_date_naive, time.min))
    end_date_ist = ist_tz.localize(datetime.combine(target_date_naive, time.max))

    # Add buffer same as provider
    start_date_ist_buf = start_date_ist - timedelta(minutes=5)
    end_date_ist_buf = end_date_ist + timedelta(minutes=5)

    from_date_str = start_date_ist_buf.strftime("%Y-%m-%d %H:%M:%S")
    to_date_str = end_date_ist_buf.strftime("%Y-%m-%d %H:%M:%S")

    logger.info(f"Fetching Dhan data from {from_date_str} to {to_date_str}")

    # Resolve Security ID
    # For HDFCBANK, exchange ID is likely NSE. We use NSE_EQ.
    # Security ID for HDFCBANK is 1333.
    # To be generic, we should fetch it, but for this script we can hardcode or query map if needed.
    # Let's try to infer from provider logic or assume 1333 for HDFCBANK as known.
    # Actually, let's use the method:

    # 1. Connect Redis to get mapping?
    # Or just hardcode for HDFCBANK for the specific task requested?
    # "do it only for HDFCBANK"
    security_id = "1333"
    exchange_segment = "NSE_EQ"

    # If symbol is different, warn user
    if "HDFCBANK" not in instrument.symbol:
        logger.warning(
            f"Warning: Script assumes HDFCBANK (ID 1333). Selected instrument is {instrument.symbol}."
        )
        # Try to use symbol as ID if it is numeric? No.

    payload = {
        "securityId": security_id,
        "exchangeSegment": exchange_segment,
        "instrument": "EQUITY",
        "interval": "5",
        "oi": False,
        "fromDate": from_date_str,
        "toDate": to_date_str,
    }

    try:
        data = await provider._make_request("/charts/intraday", payload)

        if not data or "timestamp" not in data:
            logger.error(f"No data returned from Dhan: {data}")
            return pd.DataFrame()

        history = []
        timestamps = data.get("timestamp", [])  # Epoch
        opens = data.get("open", [])
        highs = data.get("high", [])
        lows = data.get("low", [])
        closes = data.get("close", [])
        volumes = data.get("volume", [])

        for i in range(len(timestamps)):
            ts = timestamps[i]  # Epoch seconds

            # Dhan returns timestamps.
            # "Dhan sends timestamps in IST (Indian Standard Time) but as a Unix timestamp relative to UTC epoch."
            # "Fix: We subtract 19800 seconds from the received timestamp to get the correct UTC timestamp."
            # HOWEVER, in `get_intraday_prices` (which I read earlier), it had:
            # "Dhan Historical API returns valid UTC timestamp. No shift needed."
            # Let's VERIFY this behavior by comparing.

            history.append(
                {
                    "timestamp_dhan_raw": ts,
                    "open_dhan": opens[i],
                    "high_dhan": highs[i],
                    "low_dhan": lows[i],
                    "close_dhan": closes[i],
                    "volume_dhan": volumes[i],
                }
            )

        df = pd.DataFrame(history)
        if not df.empty:
            # We don't set index yet, we return raw to decide how to join in main
            pass

        return df

    except Exception as e:
        logger.error(f"Error fetching Dhan data: {e}")
        return pd.DataFrame()


async def main():
    logger.info("Starting comparison script...")

    try:
        target_date = datetime.strptime(DATE_TO_CHECK_STR, "%Y-%m-%d").date()
    except ValueError:
        logger.error("Invalid date format. Use YYYY-MM-DD")
        return

    # 1. Get DB Data
    instrument, df_db = await get_instrument_and_data_from_db(
        SYMBOL_TO_CHECK, target_date
    )

    if not instrument:
        return

    logger.info(f"DB Records: {len(df_db)}")

    # 2. Get Dhan Data
    df_dhan = await get_dhan_data(instrument, target_date)
    logger.info(f"Dhan Records: {len(df_dhan)}")

    if df_dhan.empty:
        logger.error("No Dhan data fetched. Exiting.")
        return

    # 3. Join and Compare
    # Try joining on standard epoch first
    # DB Index is 'timestamp_epoch'

    # Check if Dhan raw timestamps look like Shifted or Standard
    # Take first DB timestamp
    sample_db_ts = df_db.index[0] if not df_db.empty else 0
    sample_dhan_ts = df_dhan["timestamp_dhan_raw"].iloc[0] if not df_dhan.empty else 0

    # If they are close (within 1000s), it's standard.
    # If they differ by ~19800, it's shifted.

    join_key = "timestamp_epoch"
    dhan_key = "timestamp_dhan_raw"

    # Prepare Dhan DF for join
    df_dhan_join = df_dhan.copy()
    df_dhan_join.set_index("timestamp_dhan_raw", inplace=True)

    merged = df_db.join(df_dhan_join, how="outer", rsuffix="_dhan")

    # Check match rate
    matches = merged["open_dhan"].count()
    logger.info(f"Matches using Standard Epoch: {matches}")

    if matches == 0 and not df_db.empty and not df_dhan.empty:
        logger.info(
            "No matches with Standard Epoch. Trying Shifted Epoch (DB converted to NSE-shifted)..."
        )

        # Reset and try joining DB's shifted column with Dhan's raw
        df_db_shifted = df_db.reset_index().set_index("timestamp_shifted")
        merged_shifted = df_db_shifted.join(df_dhan_join, how="outer", rsuffix="_dhan")

        matches_shifted = merged_shifted["open_dhan"].count()
        logger.info(f"Matches using Shifted Epoch: {matches_shifted}")

        if matches_shifted > matches:
            logger.info("Using Shifted Epoch alignment (Dhan returns IST-based Epoch).")
            merged = merged_shifted
            # We should probably map the index back to standard epoch for display or just use 'timestamp_ist' from DB part

    # Calculate differences
    merged["diff_open"] = merged["open_db"] - merged["open_dhan"]
    merged["diff_close"] = merged["close_db"] - merged["close_dhan"]
    merged["diff_vol"] = merged["volume_db"] - merged["volume_dhan"]

    # % Diff
    merged["pct_diff_close"] = (merged["diff_close"] / merged["close_dhan"]) * 100
    merged["pct_diff_vol"] = (merged["diff_vol"] / merged["volume_dhan"]) * 100

    # Display Results
    pd.set_option("display.max_rows", None)
    pd.set_option("display.max_columns", None)
    pd.set_option("display.width", 1000)

    # Filter to show where we have data in both or at least one
    logger.info("\nComparison Results (Sample):")

    # Add human readable time for index
    # merged['time_utc'] = pd.to_datetime(merged.index, unit='s', utc=True)
    # merged['time_ist'] = merged['time_utc'].dt.tz_convert('Asia/Kolkata')

    # Convert index to datetime column for display
    # (Handling potential NaNs in index if join failed badly?)
    timestamps = merged.index.to_series()
    merged["time_str"] = timestamps.apply(
        lambda x: datetime.fromtimestamp(x, tz=timezone.utc)
        .astimezone(pytz.timezone("Asia/Kolkata"))
        .strftime("%H:%M:%S")
    )

    columns = [
        "time_str",
        "open_db",
        "open_dhan",
        "close_db",
        "close_dhan",
        "pct_diff_close",
        "volume_db",
        "volume_dhan",
        "diff_vol",
    ]
    print(merged[columns])

    # Summary
    print("\n--- Summary ---")
    print(f"Total Combined Rows: {len(merged)}")
    print(f"Rows with DB data: {merged['open_db'].count()}")
    print(f"Rows with Dhan data: {merged['open_dhan'].count()}")

    missing_in_db = merged[merged["open_db"].isna()]
    missing_in_dhan = merged[merged["open_dhan"].isna()]

    print(f"Missing in DB: {len(missing_in_db)}")
    print(f"Missing in Dhan: {len(missing_in_dhan)}")

    if len(missing_in_db) > 0:
        print("\ntimestamps missing in DB:")
        print(missing_in_db["time_str"].tolist())

    # Check max difference
    max_diff_close = merged["pct_diff_close"].abs().max()
    print(f"\nMax Close Price Diff %: {max_diff_close:.4f}%")


if __name__ == "__main__":
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())
