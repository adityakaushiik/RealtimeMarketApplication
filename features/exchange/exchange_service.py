from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from models import Exchange
from features.exchange.exchange_schema import ExchangeCreateOrUpdate


async def create_exchange(
    session: AsyncSession,
    exchange_data: ExchangeCreateOrUpdate,
):
    """Create a new exchange"""
    new_exchange = Exchange(
        name=exchange_data.name,
        code=exchange_data.code,
        timezone=exchange_data.timezone,
        country=exchange_data.country,
        currency=exchange_data.currency,
    )
    session.add(new_exchange)
    await session.commit()
    await session.refresh(new_exchange)
    return new_exchange


async def get_exchange_by_id(
    session: AsyncSession,
    exchange_id: int,
):
    """Get exchange by ID"""
    result = await session.execute(
        select(Exchange).where(Exchange.id == exchange_id)
    )
    exchange = result.scalar_one_or_none()
    if exchange:
        return exchange
    return None


async def get_exchange_by_code(
    session: AsyncSession,
    code: str,
):
    """Get exchange by code"""
    result = await session.execute(
        select(Exchange).where(Exchange.code == code)
    )
    exchange = result.scalar_one_or_none()
    if exchange:
        return exchange
    return None


async def get_all_exchanges(
    session: AsyncSession,
) -> list:
    """Get all exchanges"""
    result = await session.execute(select(Exchange))
    return list(result.scalars().all())


async def update_exchange(
    session: AsyncSession,
    exchange_id: int,
    exchange_data: ExchangeCreateOrUpdate,
):
    """Update an exchange"""
    result = await session.execute(
        select(Exchange).where(Exchange.id == exchange_id)
    )
    exchange = result.scalar_one_or_none()
    if not exchange:
        return None

    update_data = exchange_data.model_dump(exclude_unset=True)
    for key, value in update_data.items():
        setattr(exchange, key, value)

    await session.commit()
    await session.refresh(exchange)
    return exchange


async def delete_exchange(
    session: AsyncSession,
    exchange_id: int,
) -> bool:
    """Delete an exchange"""
    result = await session.execute(
        select(Exchange).where(Exchange.id == exchange_id)
    )
    exchange = result.scalar_one_or_none()
    if not exchange:
        return False

    await session.delete(exchange)
    await session.commit()
    return True

