import yfinance as yf
from sqlalchemy.ext.asyncio import AsyncSession

from models import PriceHistoryDaily


async def populate_instrument_in_database(
        session: AsyncSession,
        instrument_id: int,
        symbol: str,
        search_code: str,
):

    history = yf.Ticker(search_code).history(
        period="30d",
    )

    print(symbol)

    bulk_data = []

    for index, row in history.iterrows():
        print(index)
        price_history = PriceHistoryDaily(
            instrument_id=instrument_id,
            datetime=index
        )
        price_history.open = float(row["Open"])
        price_history.high = float(row["High"])
        price_history.low = float(row["Low"])
        price_history.close = float(row["Close"])
        price_history.dividend = float(row["Dividends"])
        price_history.split = float(row["Stock Splits"])
        price_history.volume = int(row["Volume"])
        bulk_data.append(price_history)

    session.add_all(bulk_data)
    await session.commit()