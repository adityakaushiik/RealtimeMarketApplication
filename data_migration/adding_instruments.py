from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from config.database_config import get_database_engine
from config.settings import get_settings
import pandas as pd

from models import Instrument, Exchange, Sector, InstrumentType


async def add_instruments():
    file_path = "C:/Users/tech/Result_13.csv"

    # Read csv file and add instruments to the database
    df = pd.read_csv(file_path)

    engine = get_database_engine()
    async with AsyncSession(engine) as session:
        for row in df.itertuples():
            symbol = row[1]
            instrument_name = row[2]
            sector_name = row[3]
            instrument_type_name = row[4]
            exchange_name = row[5]

            exchange_id = await session.scalar(
                select(Exchange.id).where(Exchange.code == exchange_name)
            )
            instrument_type_id = await session.scalar(
                select(InstrumentType.id).where(
                    InstrumentType.name == instrument_type_name
                )
            )
            sector_id = (
                await session.scalar(
                    select(Sector.id).where(Sector.name == sector_name)
                )
                if sector_name
                else None
            )

            instrument = Instrument(
                name=instrument_name,
                symbol=symbol,
                exchange_id=exchange_id,
                instrument_type_id=instrument_type_id,
                sector_id=sector_id,
                is_active=True,
                blacklisted=False,
                delisted=False,
            )
            session.add(instrument)

        await session.commit()


import asyncio

if __name__ == "__main__":
    settings = get_settings()
    asyncio.run(add_instruments())
