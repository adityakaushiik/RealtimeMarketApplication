"""
Database setup script for multi-provider configuration.
Run this script to populate provider and exchange-provider mappings.

Usage:
    python z_data_migration/setup_providers.py
"""

import asyncio
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from config.database_config import get_database_engine
from config.logger import logger
from models import Provider, Exchange, ExchangeProviderMapping


async def setup_providers():
    """
    Set up providers and their exchange mappings in the database.
    This creates the necessary records for multi-provider support.
    """
    engine = get_database_engine()

    async with AsyncSession(engine) as session:
        logger.info("Starting provider setup...")

        # 1. Create or update Yahoo Finance provider
        result = await session.execute(select(Provider).where(Provider.code == "YF"))
        yf_provider = result.scalar_one_or_none()

        if not yf_provider:
            yf_provider = Provider(
                name="Yahoo Finance",
                code="YF",
                rate_limit=2000,  # requests per minute
                credentials={},
                is_active=True,
            )
            session.add(yf_provider)
            logger.info("✅ Created Yahoo Finance provider")
        else:
            logger.info("ℹ️  Yahoo Finance provider already exists")

        # 2. Create or update Dhan provider
        result = await session.execute(select(Provider).where(Provider.code == "DHAN"))
        dhan_provider = result.scalar_one_or_none()

        if not dhan_provider:
            dhan_provider = Provider(
                name="Dhan",
                code="DHAN",
                rate_limit=1000,  # requests per minute (adjust based on Dhan's limits)
                credentials={},  # Will be loaded from env variables
                is_active=True,
            )
            session.add(dhan_provider)
            logger.info("✅ Created Dhan provider")
        else:
            logger.info("ℹ️  Dhan provider already exists")

        await session.commit()
        await session.refresh(yf_provider)
        await session.refresh(dhan_provider)

        # 3. Map Indian exchanges to Dhan
        indian_exchanges = ["NSE", "BSE"]
        for exchange_code in indian_exchanges:
            result = await session.execute(
                select(Exchange).where(Exchange.code == exchange_code)
            )
            exchange = result.scalar_one_or_none()

            if exchange:
                # Check if mapping exists
                result = await session.execute(
                    select(ExchangeProviderMapping).where(
                        ExchangeProviderMapping.provider_id == dhan_provider.id,
                        ExchangeProviderMapping.exchange_id == exchange.id,
                    )
                )
                existing_mapping = result.scalar_one_or_none()

                if not existing_mapping:
                    mapping = ExchangeProviderMapping(
                        provider_id=dhan_provider.id,
                        exchange_id=exchange.id,
                        is_active=True,
                        is_primary=True,
                    )
                    session.add(mapping)
                    logger.info(f"✅ Mapped {exchange_code} → Dhan (primary)")
                else:
                    logger.info(f"ℹ️  Mapping {exchange_code} → Dhan already exists")
            else:
                logger.warning(f"⚠️  Exchange {exchange_code} not found in database")

        # 4. Map US exchanges to Yahoo Finance
        us_exchanges = ["NYSE", "NASDAQ"]
        for exchange_code in us_exchanges:
            result = await session.execute(
                select(Exchange).where(Exchange.code == exchange_code)
            )
            exchange = result.scalar_one_or_none()

            if exchange:
                # Check if mapping exists
                result = await session.execute(
                    select(ExchangeProviderMapping).where(
                        ExchangeProviderMapping.provider_id == yf_provider.id,
                        ExchangeProviderMapping.exchange_id == exchange.id,
                    )
                )
                existing_mapping = result.scalar_one_or_none()

                if not existing_mapping:
                    mapping = ExchangeProviderMapping(
                        provider_id=yf_provider.id,
                        exchange_id=exchange.id,
                        is_active=True,
                        is_primary=True,
                    )
                    session.add(mapping)
                    logger.info(f"✅ Mapped {exchange_code} → Yahoo Finance (primary)")
                else:
                    logger.info(
                        f"ℹ️  Mapping {exchange_code} → Yahoo Finance already exists"
                    )
            else:
                logger.warning(f"⚠️  Exchange {exchange_code} not found in database")

        await session.commit()
        logger.info("🎉 Provider setup completed successfully!")

        # 5. Display summary
        logger.info("\n" + "=" * 60)
        logger.info("PROVIDER CONFIGURATION SUMMARY")
        logger.info("=" * 60)

        # Query all mappings
        result = await session.execute(
            select(
                Exchange.code,
                Provider.name,
                Provider.code,
                ExchangeProviderMapping.is_primary,
                ExchangeProviderMapping.is_active,
            )
            .join(
                ExchangeProviderMapping,
                Exchange.id == ExchangeProviderMapping.exchange_id,
            )
            .join(Provider, Provider.id == ExchangeProviderMapping.provider_id)
            .where(ExchangeProviderMapping.is_active == True)
            .order_by(Exchange.code)
        )

        mappings = result.all()
        for mapping in mappings:
            primary_label = "PRIMARY" if mapping.is_primary else "BACKUP"
            logger.info(
                f"{mapping.code:10} → {mapping.name:20} ({mapping.code_1}) [{primary_label}]"
            )

        logger.info("=" * 60)


async def verify_setup():
    """Verify that the provider setup is correct"""
    engine = get_database_engine()

    async with AsyncSession(engine) as session:
        logger.info("\n" + "=" * 60)
        logger.info("VERIFICATION")
        logger.info("=" * 60)

        # Count providers
        result = await session.execute(
            select(Provider).where(Provider.is_active == True)
        )
        providers = result.scalars().all()
        logger.info(f"Active Providers: {len(providers)}")
        for provider in providers:
            logger.info(f"  - {provider.name} ({provider.code})")

        # Count mappings
        result = await session.execute(
            select(ExchangeProviderMapping).where(
                ExchangeProviderMapping.is_active == True
            )
        )
        mappings = result.scalars().all()
        logger.info(f"\nActive Exchange-Provider Mappings: {len(mappings)}")

        logger.info("=" * 60)


if __name__ == "__main__":
    logger.info("🚀 Starting multi-provider database setup...")
    asyncio.run(setup_providers())
    asyncio.run(verify_setup())
    logger.info(
        "\n✅ Setup complete! You can now start the application with multi-provider support."
    )
    logger.info("\nNext steps:")
    logger.info("1. Add provider_instrument_mappings for your instruments")
    logger.info("2. Set DHAN_CLIENT_ID and DHAN_ACCESS_TOKEN in your .env file")
    logger.info("3. Start your application")
