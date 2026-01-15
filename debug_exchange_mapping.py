import asyncio
from sqlalchemy import select
from config.database_config import get_db_session
from models import Exchange, Provider, ExchangeProviderMapping


async def debug_mappings():
    async for session in get_db_session():
        print("--- Checking Exchange 7 ---")
        exchange = await session.get(Exchange, 7)
        if exchange:
            print(f"Exchange 7: {exchange.name} ({exchange.code})")
        else:
            print("Exchange 7 not found!")
            return

        print("\n--- Checking Mappings for Exchange 7 ---")
        stmt = (
            select(ExchangeProviderMapping, Provider)
            .join(Provider)
            .where(ExchangeProviderMapping.exchange_id == 7)
        )
        result = await session.execute(stmt)
        mappings = result.all()

        if not mappings:
            print("No mappings found for Exchange 7!")

        for mapping, provider in mappings:
            print(f"Provider: {provider.name} ({provider.code})")
            # print(f"  Mapping ID: {mapping.id}") # No ID column
            print(f"  Is Active: {mapping.is_active}")
            print(f"  Is Primary: {mapping.is_primary}")
            print(f"  Provider Active: {provider.is_active}")
            print("-" * 20)


if __name__ == "__main__":
    asyncio.run(debug_mappings())
