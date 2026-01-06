from sqlalchemy import text
from config.database_config import get_db_session
import asyncio

async def fix_version():
    async for session in get_db_session():
        print("Deleting corrupted version from alembic_version table...")
        await session.execute(text("DELETE FROM alembic_version"))
        await session.commit()
        print("Done.")

if __name__ == "__main__":
    asyncio.run(fix_version())

