from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from models import Exchange, ExchangeProviderMapping
from features.exchange.exchange_schema import ExchangeCreate, ExchangeUpdate, ExchangeInDb, \
    ExchangeProviderMappingCreate, ExchangeProviderMappingUpdate, ExchangeProviderMappingInDb
from services.data.exchange_data import ExchangeData


async def create_exchange(
        session: AsyncSession,
        exchange_data: ExchangeCreate,
):
    """Create a new exchange"""
    new_exchange = Exchange(
        name=exchange_data.name,
        code=exchange_data.code,
        timezone=exchange_data.timezone,
        country=exchange_data.country,
        currency=exchange_data.currency,
        pre_market_open_time=exchange_data.pre_market_open_time,
        market_open_time=exchange_data.market_open_time,
        market_close_time=exchange_data.market_close_time,
        post_market_close_time=exchange_data.post_market_close_time,
    )
    session.add(new_exchange)
    await session.commit()
    await session.refresh(new_exchange)
    return ExchangeInDb(
        id=new_exchange.id,
        name=new_exchange.name,
        code=new_exchange.code,
        timezone=new_exchange.timezone,
        country=new_exchange.country,
        currency=new_exchange.currency,
        pre_market_open_time=new_exchange.pre_market_open_time,
        market_open_time=new_exchange.market_open_time,
        market_close_time=new_exchange.market_close_time,
        post_market_close_time=new_exchange.post_market_close_time,
    )


async def get_exchange_by_id(
        session: AsyncSession,
        exchange_id: int,
):
    """Get exchange by ID"""
    result = await session.execute(select(Exchange).where(Exchange.id == exchange_id))
    exchange = result.scalar_one_or_none()
    if exchange:
        return ExchangeInDb(
            id=exchange.id,
            name=exchange.name,
            code=exchange.code,
            timezone=exchange.timezone,
            country=exchange.country,
            currency=exchange.currency,
        )
    return None


async def get_exchange_by_code(
        session: AsyncSession,
        code: str,
):
    """Get exchange by code"""
    result = await session.execute(select(Exchange).where(Exchange.code == code))
    exchange = result.scalar_one_or_none()
    if exchange:
        return ExchangeInDb(
            id=exchange.id,
            name=exchange.name,
            code=exchange.code,
            timezone=exchange.timezone,
            country=exchange.country,
            currency=exchange.currency,
        )
    return None


async def get_all_exchanges(
        session: AsyncSession,
) -> list[ExchangeInDb]:
    """Get all exchanges"""
    result = await session.execute(select(Exchange).where(Exchange.is_active == True))
    exchanges = result.scalars().all()
    return [
        ExchangeInDb(
            id=exchange.id,
            name=exchange.name,
            code=exchange.code,
            timezone=exchange.timezone,
            country=exchange.country,
            currency=exchange.currency,
        )
        for exchange in exchanges
    ]


async def get_all_active_exchanges(
        session: AsyncSession,
) -> list[ExchangeData]:
    """Get all active exchanges as ExchangeData objects"""
    result = await session.execute(select(Exchange).where(
        Exchange.is_active == True
    ))
    exchanges = result.scalars().all()
    return [
        ExchangeData(
            exchange_name=exchange.name,
            exchange_id=exchange.id,
            market_open_time=exchange.market_open_time,
            market_close_time=exchange.market_close_time,
            timezone_str=exchange.timezone,
        )
        for exchange in exchanges
    ]


async def update_exchange(
        session: AsyncSession,
        exchange_id: int,
        exchange_data: ExchangeUpdate,
):
    """Update an exchange"""
    result = await session.execute(select(Exchange).where(Exchange.id == exchange_id))
    exchange = result.scalar_one_or_none()
    if not exchange:
        return None

    update_data = exchange_data.model_dump(exclude_unset=True)
    for key, value in update_data.items():
        setattr(exchange, key, value)

    await session.commit()
    await session.refresh(exchange)
    return ExchangeInDb(
        id=exchange.id,
        name=exchange.name,
        code=exchange.code,
        timezone=exchange.timezone,
        country=exchange.country,
        currency=exchange.currency,
        pre_market_open_time=exchange.pre_market_open_time,
        market_open_time=exchange.market_open_time,
        market_close_time=exchange.market_close_time,
        post_market_close_time=exchange.post_market_close_time,
    )


async def delete_exchange(
        session: AsyncSession,
        exchange_id: int,
) -> bool:
    """Delete an exchange"""
    result = await session.execute(select(Exchange).where(Exchange.id == exchange_id))
    exchange = result.scalar_one_or_none()
    if not exchange:
        return False

    await session.delete(exchange)
    await session.commit()
    return True


# ExchangeProviderMapping functions

async def create_exchange_provider_mapping(
        session: AsyncSession,
        mapping_data: ExchangeProviderMappingCreate,
) -> ExchangeProviderMappingInDb:
    """Create a new exchange-provider mapping"""
    new_mapping = ExchangeProviderMapping(
        provider_id=mapping_data.provider_id,
        exchange_id=mapping_data.exchange_id,
        is_active=mapping_data.is_active,
        is_primary=mapping_data.is_primary,
    )
    session.add(new_mapping)
    await session.commit()
    await session.refresh(new_mapping)
    return ExchangeProviderMappingInDb(
        provider_id=new_mapping.provider_id,
        exchange_id=new_mapping.exchange_id,
        is_active=new_mapping.is_active,
        is_primary=new_mapping.is_primary,
    )


async def get_mappings_for_exchange(
        session: AsyncSession,
        exchange_id: int,
) -> list[ExchangeProviderMappingInDb]:
    """Get all provider mappings for an exchange"""
    result = await session.execute(
        select(ExchangeProviderMapping).where(ExchangeProviderMapping.exchange_id == exchange_id)
    )
    mappings = result.scalars().all()
    return [
        ExchangeProviderMappingInDb(
            provider_id=mapping.provider_id,
            exchange_id=mapping.exchange_id,
            is_active=mapping.is_active,
            is_primary=mapping.is_primary,
        )
        for mapping in mappings
    ]


async def get_mappings_for_provider(
        session: AsyncSession,
        provider_id: int,
) -> list[ExchangeProviderMappingInDb]:
    """Get all exchange mappings for a provider"""
    result = await session.execute(
        select(ExchangeProviderMapping).where(ExchangeProviderMapping.provider_id == provider_id)
    )
    mappings = result.scalars().all()
    return [
        ExchangeProviderMappingInDb(
            provider_id=mapping.provider_id,
            exchange_id=mapping.exchange_id,
            is_active=mapping.is_active,
            is_primary=mapping.is_primary,
        )
        for mapping in mappings
    ]


async def update_exchange_provider_mapping(
        session: AsyncSession,
        provider_id: int,
        exchange_id: int,
        update_data: ExchangeProviderMappingUpdate,
) -> ExchangeProviderMappingInDb | None:
    """Update an exchange-provider mapping"""
    result = await session.execute(
        select(ExchangeProviderMapping).where(
            (ExchangeProviderMapping.provider_id == provider_id) &
            (ExchangeProviderMapping.exchange_id == exchange_id)
        )
    )
    mapping = result.scalar_one_or_none()
    if not mapping:
        return None

    update_dict = update_data.model_dump(exclude_unset=True)
    for key, value in update_dict.items():
        setattr(mapping, key, value)

    await session.commit()
    await session.refresh(mapping)
    return ExchangeProviderMappingInDb(
        provider_id=mapping.provider_id,
        exchange_id=mapping.exchange_id,
        is_active=mapping.is_active,
        is_primary=mapping.is_primary,
    )


async def delete_exchange_provider_mapping(
        session: AsyncSession,
        provider_id: int,
        exchange_id: int,
) -> bool:
    """Delete an exchange-provider mapping"""
    result = await session.execute(
        select(ExchangeProviderMapping).where(
            (ExchangeProviderMapping.provider_id == provider_id) &
            (ExchangeProviderMapping.exchange_id == exchange_id)
        )
    )
    mapping = result.scalar_one_or_none()
    if not mapping:
        return False

    await session.delete(mapping)
    await session.commit()
    return True
