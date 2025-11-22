"""
Quick verification script to check multi-provider setup.
Run this after setting up the database to verify everything is configured correctly.

Usage:
    python verify_setup.py
"""
import asyncio
from sqlalchemy import select

from config.database_config import get_db_session
from config.logger import logger
from models import (
    Provider,
    Exchange,
    ExchangeProviderMapping,
    Instrument,
    ProviderInstrumentMapping,
)


async def verify_providers():
    """Check if providers are configured correctly"""
    logger.info("="*70)
    logger.info("1. VERIFYING PROVIDERS")
    logger.info("="*70)

    async for session in get_db_session():
        result = await session.execute(
            select(Provider).where(Provider.is_active == True)
        )
        providers = result.scalars().all()

        if not providers:
            logger.error("❌ No active providers found!")
            logger.info("   Run: python z_data_migration/setup_providers.py")
            return False

        logger.info(f"✅ Found {len(providers)} active provider(s):")
        for provider in providers:
            logger.info(f"   • {provider.name} (code: {provider.code})")

        return True


async def verify_exchange_mappings():
    """Check if exchange-provider mappings exist"""
    logger.info("\n" + "="*70)
    logger.info("2. VERIFYING EXCHANGE-PROVIDER MAPPINGS")
    logger.info("="*70)

    async for session in get_db_session():
        result = await session.execute(
            select(
                Exchange.code.label("exchange_code"),
                Provider.code.label("provider_code"),
                Provider.name.label("provider_name"),
                ExchangeProviderMapping.is_primary,
                ExchangeProviderMapping.is_active
            )
            .join(ExchangeProviderMapping, Exchange.id == ExchangeProviderMapping.exchange_id)
            .join(Provider, Provider.id == ExchangeProviderMapping.provider_id)
            .where(ExchangeProviderMapping.is_active == True)
            .order_by(Exchange.code)
        )

        mappings = result.all()

        if not mappings:
            logger.error("❌ No exchange-provider mappings found!")
            logger.info("   Run: python z_data_migration/setup_providers.py")
            return False

        logger.info(f"✅ Found {len(mappings)} exchange-provider mapping(s):")
        for mapping in mappings:
            primary = "PRIMARY" if mapping.is_primary else "BACKUP"
            logger.info(f"   • {mapping.exchange_code:10} → {mapping.provider_name:20} [{primary}]")

        return True


async def verify_instrument_mappings():
    """Check if instrument-provider mappings exist"""
    logger.info("\n" + "="*70)
    logger.info("3. VERIFYING INSTRUMENT-PROVIDER MAPPINGS")
    logger.info("="*70)

    async for session in get_db_session():
        # Count mappings per provider
        result = await session.execute(
            select(Provider.code, Provider.name)
            .distinct()
            .join(ProviderInstrumentMapping, Provider.id == ProviderInstrumentMapping.provider_id)
        )

        providers_with_mappings = result.all()

        if not providers_with_mappings:
            logger.error("❌ No instrument-provider mappings found!")
            logger.info("   Run: python z_data_migration/populate_instrument_mappings.py")
            return False

        logger.info(f"✅ Found instrument mappings for {len(providers_with_mappings)} provider(s):")

        for provider in providers_with_mappings:
            result = await session.execute(
                select(ProviderInstrumentMapping)
                .join(Provider, Provider.id == ProviderInstrumentMapping.provider_id)
                .where(Provider.code == provider.code)
            )
            count = len(result.scalars().all())
            logger.info(f"   • {provider.name:20}: {count:5} instrument(s)")

        # Show sample mappings
        logger.info("\n   Sample mappings:")
        result = await session.execute(
            select(
                Provider.code.label("provider_code"),
                Exchange.code.label("exchange_code"),
                Instrument.symbol,
                ProviderInstrumentMapping.provider_instrument_search_code
            )
            .join(ProviderInstrumentMapping, Provider.id == ProviderInstrumentMapping.provider_id)
            .join(Instrument, Instrument.id == ProviderInstrumentMapping.instrument_id)
            .join(Exchange, Exchange.id == Instrument.exchange_id)
            .limit(5)
        )

        samples = result.all()
        for sample in samples:
            logger.info(
                f"   {sample.provider_code:6} | {sample.exchange_code:8} | "
                f"{sample.symbol:10} → {sample.provider_instrument_search_code}"
            )

        return True


async def verify_env_variables():
    """Check if required environment variables are set"""
    logger.info("\n" + "="*70)
    logger.info("4. VERIFYING ENVIRONMENT VARIABLES")
    logger.info("="*70)

    from config.settings import get_settings
    settings = get_settings()

    issues = []

    # Check Dhan credentials
    if not settings.DHAN_CLIENT_ID or settings.DHAN_CLIENT_ID == "":
        issues.append("DHAN_CLIENT_ID not set in .env file")
    else:
        logger.info(f"✅ DHAN_CLIENT_ID: {'*' * min(len(settings.DHAN_CLIENT_ID), 10)}...")

    if not settings.DHAN_ACCESS_TOKEN or settings.DHAN_ACCESS_TOKEN == "":
        issues.append("DHAN_ACCESS_TOKEN not set in .env file")
    else:
        logger.info(f"✅ DHAN_ACCESS_TOKEN: {'*' * min(len(settings.DHAN_ACCESS_TOKEN), 10)}...")

    # Check database URL
    if not settings.DATABASE_URL or settings.DATABASE_URL == "":
        issues.append("DATABASE_URL not set in .env file")
    else:
        logger.info("✅ DATABASE_URL: [configured]")

    if issues:
        logger.warning("⚠️  Environment variable issues:")
        for issue in issues:
            logger.warning(f"   • {issue}")
        logger.info("\n   Add missing variables to your .env file")
        return False

    return True


async def test_provider_imports():
    """Test if provider classes can be imported"""
    logger.info("\n" + "="*70)
    logger.info("5. VERIFYING PROVIDER IMPORTS")
    logger.info("="*70)

    try:
        from services.provider.base_provider import BaseMarketDataProvider
        logger.info("✅ BaseMarketDataProvider imported successfully")
    except Exception as e:
        logger.error(f"❌ Failed to import BaseMarketDataProvider: {e}")
        return False

    try:
        from services.provider.yahoo_provider import YahooFinanceProvider
        logger.info("✅ YahooFinanceProvider imported successfully")
    except Exception as e:
        logger.error(f"❌ Failed to import YahooFinanceProvider: {e}")
        return False

    try:
        from services.provider.dhan_provider import DhanProvider
        logger.info("✅ DhanProvider imported successfully")
    except Exception as e:
        logger.error(f"❌ Failed to import DhanProvider: {e}")
        return False

    try:
        from services.provider.provider_manager import ProviderManager
        logger.info("✅ ProviderManager imported successfully")
    except Exception as e:
        logger.error(f"❌ Failed to import ProviderManager: {e}")
        return False

    return True


async def test_provider_manager_initialization():
    """Test if ProviderManager can initialize"""
    logger.info("\n" + "="*70)
    logger.info("6. TESTING PROVIDER MANAGER INITIALIZATION")
    logger.info("="*70)

    try:
        from services.provider.provider_manager import ProviderManager
        from utils.common_constants import DataIngestionFormat

        def dummy_callback(message: DataIngestionFormat):
            pass

        manager = ProviderManager(callback=dummy_callback)
        await manager.initialize()

        logger.info(f"✅ ProviderManager initialized with {len(manager.providers)} provider(s)")

        status = manager.get_provider_status()
        for provider_code, provider_status in status.items():
            logger.info(f"   • {provider_code}: {provider_status}")

        return True
    except Exception as e:
        logger.error(f"❌ Failed to initialize ProviderManager: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return False


async def main():
    """Run all verification checks"""
    logger.info("\n" + "🚀"*35)
    logger.info("MULTI-PROVIDER SETUP VERIFICATION")
    logger.info("🚀"*35 + "\n")

    checks = [
        ("Providers", verify_providers),
        ("Exchange Mappings", verify_exchange_mappings),
        ("Instrument Mappings", verify_instrument_mappings),
        ("Environment Variables", verify_env_variables),
        ("Provider Imports", test_provider_imports),
        ("Provider Manager", test_provider_manager_initialization),
    ]

    results = {}

    for check_name, check_func in checks:
        try:
            result = await check_func()
            results[check_name] = result
        except Exception as e:
            logger.error(f"❌ Check '{check_name}' failed with error: {e}")
            import traceback
            logger.error(traceback.format_exc())
            results[check_name] = False

    # Summary
    logger.info("\n" + "="*70)
    logger.info("VERIFICATION SUMMARY")
    logger.info("="*70)

    all_passed = True
    for check_name, result in results.items():
        status = "✅ PASS" if result else "❌ FAIL"
        logger.info(f"{status:10} | {check_name}")
        if not result:
            all_passed = False

    logger.info("="*70)

    if all_passed:
        logger.info("\n🎉 ALL CHECKS PASSED! Your multi-provider setup is ready.")
        logger.info("\nNext steps:")
        logger.info("1. Start your application: python main.py")
        logger.info("2. Monitor logs for provider connections")
        logger.info("3. Check provider status via API endpoint")
    else:
        logger.warning("\n⚠️  SOME CHECKS FAILED. Please fix the issues above.")
        logger.info("\nCommon fixes:")
        logger.info("1. Run: python z_data_migration/setup_providers.py")
        logger.info("2. Run: python z_data_migration/populate_instrument_mappings.py")
        logger.info("3. Add missing environment variables to .env file")
        logger.info("4. Ensure dhanhq package is installed: pip install dhanhq==2.0.2")

    logger.info("\n" + "="*70 + "\n")


if __name__ == "__main__":
    asyncio.run(main())

