import re
from typing import Optional, AsyncGenerator

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, create_async_engine

from config.settings import get_settings

_DATABASE_ENGINE: Optional[AsyncEngine] = None


def _build_async_database_url(database_url: str) -> str:
    return re.sub(r"^postgresql:", "postgresql+asyncpg:", database_url, count=1)


def get_database_engine(echo: bool = False) -> AsyncEngine:
    settings = get_settings()

    global _DATABASE_ENGINE
    if _DATABASE_ENGINE is None:
        database_url = settings.DATABASE_URL
        if not database_url:
            raise RuntimeError("DATABASE_URL environment variable is not set.")
        _DATABASE_ENGINE = create_async_engine(
            _build_async_database_url(database_url),
            echo=echo,
            pool_pre_ping=True,
        )
    return _DATABASE_ENGINE


async def check_database_connection() -> None:
    engine = get_database_engine()
    async with engine.connect() as conn:
        result = await conn.execute(text("select 'hello world'"))
        print(result.fetchall())


async def close_database_engine() -> None:
    global _DATABASE_ENGINE
    if _DATABASE_ENGINE is not None:
        await _DATABASE_ENGINE.dispose()
        _DATABASE_ENGINE = None


async def get_db_session() -> AsyncGenerator[AsyncSession, None]:
    engine = get_database_engine()
    async with AsyncSession(engine) as session:
        yield session
