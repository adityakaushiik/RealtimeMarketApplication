import io
import pandas as pd
import yfinance as yf
from fastapi import UploadFile, HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from datetime import timezone

from models import PriceHistoryDaily, PriceHistoryIntraday, Instrument


async def populate_instrument_in_database(
    session: AsyncSession,
    instrument_id: int,
    symbol: str,
    search_code: str,
):
    history = yf.Ticker(search_code).history(
        period="6d",
    )

    print(symbol)

    bulk_data = []

    for index, row in history.iterrows():
        print(index)

        result = await session.execute(
            select(PriceHistoryDaily).where(
                PriceHistoryDaily.instrument_id == instrument_id,
                PriceHistoryDaily.datetime == index,
            )
        )
        price_history = result.scalar_one_or_none()

        if price_history is None:
            price_history = PriceHistoryDaily(
                instrument_id=instrument_id, datetime=pd.to_datetime(index).to_pydatetime()
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


def generate_backfill_template() -> io.BytesIO:
    df = pd.DataFrame(columns=[
        "symbol", "datetime", "open", "high", "low", "close",
        "volume", "timeframe", "dividend", "split",
        "previous_close", "adj_close", "deliver_percentage", "split_adjusted"
    ])
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Backfill Template')
    output.seek(0)
    return output


async def process_backfill_file(session: AsyncSession, file: UploadFile):
    if not file.filename.endswith(('.xlsx', '.xls')):
        raise HTTPException(status_code=400, detail="Invalid file format. Please upload an Excel file.")

    contents = await file.read()
    try:
        df = pd.read_excel(io.BytesIO(contents))
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error reading Excel file: {str(e)}")

    # Validate columns
    required_columns = ["symbol", "datetime", "open", "high", "low", "close", "timeframe"]
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        raise HTTPException(status_code=400, detail=f"Missing required columns: {', '.join(missing_columns)}")

    # Cache instruments
    instruments_result = await session.execute(select(Instrument.symbol, Instrument.id))
    instrument_map = {row.symbol: row.id for row in instruments_result.all()}

    daily_count = 0
    intraday_count = 0
    errors = []

    for index, row in df.iterrows():
        symbol = row.get('symbol')
        if symbol not in instrument_map:
            errors.append(f"Row {index+2}: Symbol '{symbol}' not found.")
            continue

        instrument_id = instrument_map[symbol]

        try:
            dt = pd.to_datetime(row['datetime']).to_pydatetime()
            # Ensure timezone awareness if needed. Assuming UTC for now if naive.
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)

            timeframe = str(row['timeframe']).lower()

            # Prepare values
            open_val = float(row['open'])
            high_val = float(row['high'])
            low_val = float(row['low'])
            close_val = float(row['close'])
            volume_val = int(row['volume']) if pd.notna(row.get('volume')) else None

            # Optional fields
            previous_close_val = float(row['previous_close']) if pd.notna(row.get('previous_close')) else None
            adj_close_val = float(row['adj_close']) if pd.notna(row.get('adj_close')) else None
            deliver_percentage_val = float(row['deliver_percentage']) if pd.notna(row.get('deliver_percentage')) else None

            if timeframe == '1d':
                dividend_val = float(row['dividend']) if pd.notna(row.get('dividend')) else 0.0
                split_val = float(row['split']) if pd.notna(row.get('split')) else 0.0
                split_adjusted_val = float(row['split_adjusted']) if pd.notna(row.get('split_adjusted')) else None

                stmt = select(PriceHistoryDaily).where(
                    PriceHistoryDaily.instrument_id == instrument_id,
                    PriceHistoryDaily.datetime == dt
                )
                result = await session.execute(stmt)
                existing = result.scalar_one_or_none()

                if existing:
                    existing.open = open_val
                    existing.high = high_val
                    existing.low = low_val
                    existing.close = close_val
                    existing.volume = volume_val
                    existing.dividend = dividend_val
                    existing.split = split_val
                    existing.previous_close = previous_close_val
                    existing.adj_close = adj_close_val
                    existing.deliver_percentage = deliver_percentage_val
                    existing.split_adjusted = split_adjusted_val
                else:
                    new_record = PriceHistoryDaily(
                        instrument_id=instrument_id,
                        datetime=dt,
                        open=open_val,
                        high=high_val,
                        low=low_val,
                        close=close_val,
                        volume=volume_val,
                        dividend=dividend_val,
                        split=split_val,
                        previous_close=previous_close_val,
                        adj_close=adj_close_val,
                        deliver_percentage=deliver_percentage_val,
                        split_adjusted=split_adjusted_val
                    )
                    session.add(new_record)
                daily_count += 1

            else:
                # Intraday
                stmt = select(PriceHistoryIntraday).where(
                    PriceHistoryIntraday.instrument_id == instrument_id,
                    PriceHistoryIntraday.datetime == dt
                )
                result = await session.execute(stmt)
                existing = result.scalar_one_or_none()

                if existing:
                    existing.open = open_val
                    existing.high = high_val
                    existing.low = low_val
                    existing.close = close_val
                    existing.volume = volume_val
                    existing.interval = timeframe
                    existing.previous_close = previous_close_val
                    existing.adj_close = adj_close_val
                    existing.deliver_percentage = deliver_percentage_val
                else:
                    new_record = PriceHistoryIntraday(
                        instrument_id=instrument_id,
                        datetime=dt,
                        open=open_val,
                        high=high_val,
                        low=low_val,
                        close=close_val,
                        volume=volume_val,
                        interval=timeframe,
                        previous_close=previous_close_val,
                        adj_close=adj_close_val,
                        deliver_percentage=deliver_percentage_val
                    )
                    session.add(new_record)
                intraday_count += 1

        except Exception as e:
            errors.append(f"Row {index+2}: Error processing data - {str(e)}")

    if errors:
        raise HTTPException(status_code=400, detail={"message": "Validation errors found", "errors": errors})

    await session.commit()

    return {
        "message": "Backfill completed successfully",
        "daily_records_processed": daily_count,
        "intraday_records_processed": intraday_count
    }
