"""
Timestamp Investigation: Comparing DB timestamps with Dhan REST API timestamps

This script investigates why the backfill didn't update any records.
The issue is likely a timestamp mismatch between how we store data
and how Dhan REST API returns it.
"""

import asyncio
import sys
from datetime import datetime, timedelta, timezone, date
import pytz
import requests

sys.path.insert(0, r"C:\Users\tech\OneDrive\Documents\GitHub\RealtimeMarketApplication")

from sqlalchemy import select
from sqlalchemy.orm import joinedload

from config.database_config import get_db_session
from config.settings import get_settings
from models import Instrument, PriceHistoryIntraday, ProviderInstrumentMapping, Provider


async def investigate_timestamps():
    """Investigate timestamp differences between DB and Dhan REST API."""

    print("=" * 70)
    print("🔍 TIMESTAMP INVESTIGATION")
    print("=" * 70)

    settings = get_settings()
    ist = pytz.timezone('Asia/Kolkata')

    async for session in get_db_session():
        # Get instrument and Dhan security ID
        stmt = (
            select(Instrument, ProviderInstrumentMapping.provider_instrument_search_code)
            .options(joinedload(Instrument.exchange))
            .join(ProviderInstrumentMapping, Instrument.id == ProviderInstrumentMapping.instrument_id)
            .join(Provider, ProviderInstrumentMapping.provider_id == Provider.id)
            .where(
                Instrument.symbol == "KOTAKBANK",
                Provider.code == "DHAN"
            )
        )
        result = await session.execute(stmt)
        row = result.first()

        if not row:
            print("❌ No Dhan mapping found for KOTAKBANK")
            return

        instrument, security_id = row
        print(f"✅ KOTAKBANK -> Dhan Security ID: {security_id}")

        # Fetch candles from Dhan REST API
        print(f"\n📡 Fetching candles from Dhan REST API...")

        url = "https://api.dhan.co/v2/charts/intraday"
        headers = {
            "Content-Type": "application/json",
            "access-token": settings.DHAN_ACCESS_TOKEN,
            "Accept": "application/json",
        }

        today = date.today()
        payload = {
            "securityId": security_id,
            "exchangeSegment": "NSE_EQ",
            "instrument": "EQUITY",
            "interval": "5",
            "fromDate": today.strftime("%Y-%m-%d"),
            "toDate": today.strftime("%Y-%m-%d"),
        }

        response = requests.post(url, json=payload, headers=headers)

        if not response.ok:
            print(f"❌ API Error: {response.status_code} - {response.text}")
            return

        data = response.json()

        # Parse response
        timestamps = data.get("timestamp", [])
        opens = data.get("open", [])

        print(f"✅ Received {len(timestamps)} candles from API")

        if timestamps:
            print(f"\n📊 DHAN REST API TIMESTAMPS (first 5):")
            print("-" * 80)

            for i in range(min(5, len(timestamps))):
                ts = timestamps[i]

                # Raw epoch
                raw_dt = datetime.fromtimestamp(ts, tz=timezone.utc)

                # If Dhan sends IST as UTC epoch
                ist_adjusted = raw_dt - timedelta(hours=5, minutes=30)

                # If timestamp is already in UTC
                direct_ist = raw_dt.astimezone(ist)

                print(f"   Raw epoch: {ts}")
                print(f"   As UTC: {raw_dt}")
                print(f"   As IST (displayed): {direct_ist}")
                print(f"   Adjusted (IST->UTC): {ist_adjusted}")
                print(f"   Open price: {opens[i] if i < len(opens) else 'N/A'}")
                print()

        # Now compare with database
        print(f"\n📊 DATABASE TIMESTAMPS (first 5 from today):")
        print("-" * 80)

        exchange = instrument.exchange
        market_open = exchange.market_open_time
        market_close = exchange.market_close_time

        start_dt = ist.localize(datetime.combine(today, market_open))
        end_dt = ist.localize(datetime.combine(today, market_close))

        stmt = (
            select(PriceHistoryIntraday)
            .where(
                PriceHistoryIntraday.instrument_id == instrument.id,
                PriceHistoryIntraday.datetime >= start_dt.astimezone(timezone.utc),
                PriceHistoryIntraday.datetime <= end_dt.astimezone(timezone.utc),
            )
            .order_by(PriceHistoryIntraday.datetime)
            .limit(10)
        )
        result = await session.execute(stmt)
        records = result.scalars().all()

        for r in records:
            print(f"   DB datetime (UTC): {r.datetime}")
            print(f"   As IST: {r.datetime.astimezone(ist)}")
            print(f"   Epoch (ms): {int(r.datetime.timestamp() * 1000)}")
            print(f"   OHLC: O={r.open}, H={r.high}, L={r.low}, C={r.close}")
            print()

        # Find matching timestamps
        print(f"\n🔍 TIMESTAMP MATCHING ANALYSIS:")
        print("-" * 80)

        db_epochs = set()
        for r in records:
            db_epochs.add(int(r.datetime.timestamp() * 1000))

        api_epochs_raw = set(ts * 1000 for ts in timestamps)  # Convert to ms
        api_epochs_adjusted = set((ts - 19800) * 1000 for ts in timestamps)  # IST -> UTC adjustment

        matches_raw = db_epochs & api_epochs_raw
        matches_adjusted = db_epochs & api_epochs_adjusted

        print(f"DB epochs (sample): {list(db_epochs)[:3]}")
        print(f"API epochs raw (sample): {list(api_epochs_raw)[:3]}")
        print(f"API epochs adjusted (sample): {list(api_epochs_adjusted)[:3]}")
        print()
        print(f"Matches with raw API epochs: {len(matches_raw)}")
        print(f"Matches with adjusted API epochs: {len(matches_adjusted)}")

        # Check the actual difference
        if timestamps and records:
            first_api_ts = timestamps[0]
            first_db_ts = int(records[0].datetime.timestamp())

            diff_seconds = first_api_ts - first_db_ts
            diff_hours = diff_seconds / 3600

            print(f"\n📐 DIFFERENCE ANALYSIS:")
            print(f"   First API timestamp: {first_api_ts} ({datetime.fromtimestamp(first_api_ts, tz=timezone.utc)})")
            print(f"   First DB timestamp: {first_db_ts} ({records[0].datetime})")
            print(f"   Difference: {diff_seconds} seconds ({diff_hours:.2f} hours)")

            if abs(diff_hours - 5.5) < 0.1:
                print(f"\n   ⚠️  The difference is ~5.5 hours (IST offset)")
                print(f"   This confirms Dhan REST API returns timestamps in IST")
                print(f"   But our DB stores in UTC")
                print(f"   FIX: We need to subtract 5.5 hours from API timestamps")


if __name__ == "__main__":
    asyncio.run(investigate_timestamps())

