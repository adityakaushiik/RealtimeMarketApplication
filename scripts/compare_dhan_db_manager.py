import asyncio
import os
import sys
from datetime import datetime, time, timezone
import pandas as pd
import pytz
from sqlalchemy import select

# Add project root to path
# We are in scripts/, so parent is root
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.database_config import get_db_session
from config.logger import logger
from models import Instrument, PriceHistoryIntraday
from services.provider.provider_manager import get_provider_manager

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


async def get_data_via_manager(
    instrument: Instrument, target_date_naive: datetime
) -> pd.DataFrame:
    """
    Fetches intraday data using the ProviderManager.
    """
    logger.info("Initializing ProviderManager...")
    manager = get_provider_manager()
    await manager.initialize()

    logger.info(
        f"Fetching data via ProviderManager for {instrument.symbol} on {target_date_naive}"
    )

    # Calculate IST range for start/end parameters
    ist_tz = pytz.timezone("Asia/Kolkata")
    start_date_ist = ist_tz.localize(datetime.combine(target_date_naive, time.min))
    end_date_ist = ist_tz.localize(datetime.combine(target_date_naive, time.max))

    try:
        # Using manager
        price_history_list = await manager.get_intraday_prices(instrument)

        if not price_history_list:
            logger.warning("ProviderManager returned no data.")
            return pd.DataFrame()

        logger.info(f"Received {len(price_history_list)} records from Manager.")

        # Convert list of PriceHistoryIntraday objects to DataFrame
        data = []
        for p in price_history_list:
            # We need to filter for our target date here
            # p.datetime is UTC datetime

            # Check if date matches target_date
            # Convert p.datetime (UTC) to IST date to compare??
            # Or just check if it falls in the UTC range we calculated earlier.

            # Let's convert to IST for easy date check
            p_dt_utc = (
                p.datetime.replace(tzinfo=timezone.utc)
                if p.datetime.tzinfo is None
                else p.datetime
            )
            p_dt_ist = p_dt_utc.astimezone(ist_tz)

            if p_dt_ist.date() == target_date_naive:
                # It's a match
                ts_epoch = int(p_dt_utc.timestamp())

                data.append(
                    {
                        "timestamp_dhan_raw": ts_epoch,  # Assuming provider parsed it correctly to UTC
                        "open_dhan": p.open,
                        "high_dhan": p.high,
                        "low_dhan": p.low,
                        "close_dhan": p.close,
                        "volume_dhan": p.volume,
                    }
                )

        df = pd.DataFrame(data)
        return df

    except Exception as e:
        # import traceback
        # Traceback is not available in production or some contexts, but we print string
        logger.error(f"Error fetching data via Manager: {e}")
        return pd.DataFrame()


async def main():
    logger.info("Starting comparison script (via ProviderManager)...")

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

    # 2. Get Dhan Data via Manager
    df_dhan = await get_data_via_manager(instrument, target_date)
    logger.info(f"Dhan Records (Filtered to target date): {len(df_dhan)}")

    if df_dhan.empty:
        logger.error("No Dhan data fetched via Manager. Exiting.")
    #        return
    # I will allow it to continue to show empty vs DB comparison (missing data) if any

    # 3. Join and Compare
    # Note: Since ProviderManager returns PriceHistoryIntraday objects which contain datetime objects,
    # and DhanProvider implementation ALREADY parses the timestamp to UTC:
    # "dt = datetime.fromtimestamp(ts, tz=timezone.utc)"
    # We expect `timestamp_dhan_raw` in our DF to be a clean UTC epoch.

    # However, if Dhan was returning IST-shifted epoch, and `datetime.fromtimestamp` treated it as UTC epoch,
    # the resulting UTC datetime would be wrong (shifted).
    # DhanProvider logic I read: "Dhan Historical API returns valid UTC timestamp. No shift needed."

    # So we expect direct match.

    join_key = "timestamp_epoch"
    dhan_key = "timestamp_dhan_raw"

    if df_dhan.empty:
        df_dhan_join = pd.DataFrame(
            columns=["timestamp_dhan_raw", "open_dhan", "close_dhan", "volume_dhan"]
        )
        df_dhan_join.set_index("timestamp_dhan_raw", inplace=True)
    else:
        # Prepare Dhan DF for join
        df_dhan_join = df_dhan.copy()
        df_dhan_join.set_index("timestamp_dhan_raw", inplace=True)

    merged = df_db.join(df_dhan_join, how="outer", rsuffix="_dhan")

    matches = merged["open_dhan"].count()
    logger.info(f"Matches using Manager Data (Standard Epoch): {matches}")

    if matches == 0 and not df_dhan.empty:
        logger.warning(
            "No matches found. This might indicate Timezone shift issue persisted despite Provider parsing."
        )
        # Try shifting logic again just in case?
        # If Provider returns shifted UTC datetime, we can try to align.

        df_db_shifted = df_db.reset_index().set_index("timestamp_shifted")
        merged_shifted = df_db_shifted.join(df_dhan_join, how="outer", rsuffix="_dhan")
        matches_shifted = merged_shifted["open_dhan"].count()
        logger.info(f"Matches using Shifted Epoch: {matches_shifted}")

        if matches_shifted > 0:
            merged = merged_shifted

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
    logger.info("\nComparison Results (Via Manager):")

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

    # Cleanup manager
    manager = get_provider_manager()  # Use global instance
    manager.stop_all_providers()
    manager.stop_sync_task()


if __name__ == "__main__":
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())
