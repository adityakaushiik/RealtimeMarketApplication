"""
Script to populate provider_instrument_mappings table.
This creates mappings between instruments and provider-specific symbol codes.

Usage:
    python z_data_migration/populate_instrument_mappings.py
"""
import asyncio
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from config.database_config import get_database_engine
from config.logger import logger
from models import (
    Provider,
    Exchange,
    Instrument,
    ProviderInstrumentMapping
)


async def populate_dhan_mappings():
    """
    Populate provider_instrument_mappings for Dhan provider.
    Maps NSE/BSE instruments to Dhan's symbol format.

    Dhan symbol format examples:
    - NSE: "RELIANCE-EQ", "TCS-EQ", "INFY-EQ"
    - BSE: Similar format
    """
    engine = get_database_engine()

    async with AsyncSession(engine) as session:
        logger.info("Populating Dhan instrument mappings...")

        # Get Dhan provider
        result = await session.execute(
            select(Provider).where(Provider.code == "DHAN")
        )
        dhan_provider = result.scalar_one_or_none()

        if not dhan_provider:
            logger.error("❌ Dhan provider not found. Run setup_providers.py first.")
            return

        # Get NSE and BSE exchanges
        result = await session.execute(
            select(Exchange).where(Exchange.code.in_(["NSE", "BSE"]))
        )
        indian_exchanges = {ex.code: ex for ex in result.scalars().all()}

        if not indian_exchanges:
            logger.error("❌ NSE/BSE exchanges not found in database")
            return

        # Get all active instruments from Indian exchanges
        result = await session.execute(
            select(Instrument)
            .where(
                Instrument.exchange_id.in_([ex.id for ex in indian_exchanges.values()]),
                Instrument.is_active == True,
                Instrument.blacklisted == False,
                Instrument.delisted == False
            )
        )
        instruments = result.scalars().all()

        logger.info(f"Found {len(instruments)} Indian instruments")

        added_count = 0
        skipped_count = 0

        for instrument in instruments:
            # Check if mapping already exists
            result = await session.execute(
                select(ProviderInstrumentMapping).where(
                    ProviderInstrumentMapping.provider_id == dhan_provider.id,
                    ProviderInstrumentMapping.instrument_id == instrument.id
                )
            )
            existing = result.scalar_one_or_none()

            if existing:
                skipped_count += 1
                continue

            # Create Dhan-format symbol
            # Format: SYMBOL-EQ for NSE equities
            # Adjust this logic based on your actual requirements
            dhan_symbol = f"{instrument.symbol}-EQ"

            # Create mapping
            mapping = ProviderInstrumentMapping(
                provider_id=dhan_provider.id,
                instrument_id=instrument.id,
                provider_instrument_search_code=dhan_symbol
            )
            session.add(mapping)
            added_count += 1

            if added_count % 100 == 0:
                logger.info(f"Processed {added_count} mappings...")

        await session.commit()

        logger.info(f"✅ Added {added_count} new Dhan mappings")
        logger.info(f"ℹ️  Skipped {skipped_count} existing mappings")


async def populate_yahoo_mappings():
    """
    Populate provider_instrument_mappings for Yahoo Finance provider.
    Maps US instruments to Yahoo Finance symbol format.

    Yahoo symbol format examples:
    - NYSE/NASDAQ: "AAPL", "TSLA", "GOOGL" (standard tickers)
    """
    engine = get_database_engine()

    async with AsyncSession(engine) as session:
        logger.info("Populating Yahoo Finance instrument mappings...")

        # Get Yahoo Finance provider
        result = await session.execute(
            select(Provider).where(Provider.code == "YF")
        )
        yf_provider = result.scalar_one_or_none()

        if not yf_provider:
            logger.error("❌ Yahoo Finance provider not found. Run setup_providers.py first.")
            return

        # Get US exchanges
        result = await session.execute(
            select(Exchange).where(Exchange.code.in_(["NYSE", "NASDAQ"]))
        )
        us_exchanges = {ex.code: ex for ex in result.scalars().all()}

        if not us_exchanges:
            logger.error("❌ NYSE/NASDAQ exchanges not found in database")
            return

        # Get all active instruments from US exchanges
        result = await session.execute(
            select(Instrument)
            .where(
                Instrument.exchange_id.in_([ex.id for ex in us_exchanges.values()]),
                Instrument.is_active == True,
                Instrument.blacklisted == False,
                Instrument.delisted == False
            )
        )
        instruments = result.scalars().all()

        logger.info(f"Found {len(instruments)} US instruments")

        added_count = 0
        skipped_count = 0

        for instrument in instruments:
            # Check if mapping already exists
            result = await session.execute(
                select(ProviderInstrumentMapping).where(
                    ProviderInstrumentMapping.provider_id == yf_provider.id,
                    ProviderInstrumentMapping.instrument_id == instrument.id
                )
            )
            existing = result.scalar_one_or_none()

            if existing:
                skipped_count += 1
                continue

            # Yahoo Finance uses standard ticker symbols
            yf_symbol = instrument.symbol

            # Create mapping
            mapping = ProviderInstrumentMapping(
                provider_id=yf_provider.id,
                instrument_id=instrument.id,
                provider_instrument_search_code=yf_symbol
            )
            session.add(mapping)
            added_count += 1

            if added_count % 100 == 0:
                logger.info(f"Processed {added_count} mappings...")

        await session.commit()

        logger.info(f"✅ Added {added_count} new Yahoo Finance mappings")
        logger.info(f"ℹ️  Skipped {skipped_count} existing mappings")


async def verify_mappings():
    """Verify that instrument mappings are correct"""
    engine = get_database_engine()

    async with AsyncSession(engine) as session:
        logger.info("\n" + "="*60)
        logger.info("INSTRUMENT MAPPINGS VERIFICATION")
        logger.info("="*60)

        result = await session.execute(
            select(
                Provider.name,
                Provider.code,
                Instrument.symbol.label("instrument_symbol"),
                ProviderInstrumentMapping.provider_instrument_search_code,
                Exchange.code.label("exchange_code")
            )
            .join(ProviderInstrumentMapping, Provider.id == ProviderInstrumentMapping.provider_id)
            .join(Instrument, Instrument.id == ProviderInstrumentMapping.instrument_id)
            .join(Exchange, Exchange.id == Instrument.exchange_id)
            .limit(10)  # Show first 10 examples
        )

        mappings = result.all()

        logger.info("Sample Mappings (first 10):")
        for mapping in mappings:
            logger.info(
                f"  {mapping.exchange_code:10} | {mapping.instrument_symbol:10} → "
                f"{mapping.name:20} | {mapping.provider_instrument_search_code}"
            )

        # Count total mappings per provider
        result = await session.execute(
            select(
                Provider.name,
                Provider.code,
            )
            .join(ProviderInstrumentMapping, Provider.id == ProviderInstrumentMapping.provider_id)
            .group_by(Provider.id, Provider.name, Provider.code)
        )

        logger.info("\nTotal Mappings by Provider:")

        for provider_code in ["YF", "DHAN"]:
            result = await session.execute(
                select(ProviderInstrumentMapping)
                .join(Provider, Provider.id == ProviderInstrumentMapping.provider_id)
                .where(Provider.code == provider_code)
            )
            count = len(result.scalars().all())
            logger.info(f"  {provider_code}: {count} mappings")

        logger.info("="*60)


if __name__ == "__main__":
    logger.info("🚀 Starting instrument mappings population...")

    asyncio.run(populate_dhan_mappings())
    asyncio.run(populate_yahoo_mappings())
    asyncio.run(verify_mappings())

    logger.info("\n✅ Instrument mappings setup complete!")
    logger.info("\nYour application is now ready for multi-provider support.")
    logger.info("Start the application to begin receiving data from multiple providers.")

