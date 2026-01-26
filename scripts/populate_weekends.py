import asyncio
import sys
import os
from datetime import date, timedelta
from sqlalchemy import select

# Add the parent directory to the python path so we can import modules
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from config.database_config import get_database_engine
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import sessionmaker
from models.exchange_holiday import ExchangeHoliday

async def populate_weekends(year: int, exchange_ids: list[int]):
    engine = get_database_engine()
    async_session = sessionmaker(
        engine, class_=AsyncSession, expire_on_commit=False
    )

    async with async_session() as session:
        for exchange_id in exchange_ids:
            print(f"Processing Exchange ID: {exchange_id} for Year: {year}")
            
            start_date = date(year, 1, 1)
            end_date = date(year, 12, 31)
            delta = timedelta(days=1)
            
            current_date = start_date
            added_count = 0
            skipped_count = 0
            
            while current_date <= end_date:
                # 5 is Saturday, 6 is Sunday
                if current_date.weekday() in [5, 6]:
                    # Check if holiday already exists
                    stmt = select(ExchangeHoliday).where(
                        ExchangeHoliday.exchange_id == exchange_id,
                        ExchangeHoliday.date == current_date
                    )
                    result = await session.execute(stmt)
                    existing_holiday = result.scalar_one_or_none()
                    
                    if not existing_holiday:
                        description = "Saturday" if current_date.weekday() == 5 else "Sunday"
                        new_holiday = ExchangeHoliday(
                            exchange_id=exchange_id,
                            date=current_date,
                            description=f"Weekend - {description}",
                            is_closed=True
                        )
                        session.add(new_holiday)
                        added_count += 1
                    else:
                        skipped_count += 1
                
                current_date += delta
            
            await session.commit()
            print(f"Exchange {exchange_id}: Added {added_count} weekends. Skipped {skipped_count} existing holidays.")

if __name__ == "__main__":
    # You can change these values or parse them from args
    YEAR_TO_PROCESS = 2025  # Or current year
    EXCHANGE_IDS = [1, 7, 8]
    
    # Allow passing year as argument
    if len(sys.argv) > 1:
        try:
            YEAR_TO_PROCESS = int(sys.argv[1])
        except ValueError:
            print("Usage: python populate_weekends.py [YEAR] [EXCHANGE_ID1] [EXCHANGE_ID2] ...")
            sys.exit(1)
            
    # Allow passing exchange ids as arguments
    if len(sys.argv) > 2:
        try:
            EXCHANGE_IDS = [int(arg) for arg in sys.argv[2:]]
        except ValueError:
             print("Usage: python populate_weekends.py [YEAR] [EXCHANGE_ID1] [EXCHANGE_ID2] ...")
             sys.exit(1)

    print(f"Starting weekend population for Year: {YEAR_TO_PROCESS}, Exchanges: {EXCHANGE_IDS}")
    
    # Run the async function
    try:
        if sys.platform == 'win32':
             asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        asyncio.run(populate_weekends(YEAR_TO_PROCESS, EXCHANGE_IDS))
    except KeyboardInterrupt:
        print("Script interrupted.")
    except Exception as e:
        print(f"An error occurred: {e}")
