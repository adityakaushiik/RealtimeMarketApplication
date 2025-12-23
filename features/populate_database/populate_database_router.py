from fastapi import APIRouter, Depends, HTTPException, UploadFile, File
from fastapi.responses import StreamingResponse
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from starlette.status import HTTP_200_OK
from datetime import date

from config.database_config import get_db_session
from features.auth.auth_service import require_auth
from features.exchange.exchange_service import (
    get_exchange_by_code,
    get_all_active_exchanges,
)
from features.populate_database.populate_database_service import (
    populate_instrument_in_database,
    generate_backfill_template,
    process_backfill_file,
)
from models import Instrument, ProviderInstrumentMapping
from services.data.data_creation import DataCreationService
from utils.common_constants import UserRoles

populate_database_route = APIRouter(
    prefix="/populate_database",
    tags=["populate_database"],
)


@populate_database_route.get("/backfill/template")
async def get_backfill_template(
    user_claims: dict = Depends(require_auth([UserRoles.ADMIN])),
):
    file_stream = generate_backfill_template()

    return StreamingResponse(
        file_stream,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": "attachment; filename=backfill_template.xlsx"},
    )


@populate_database_route.post("/backfill")
async def backfill_data(
    file: UploadFile = File(...),
    session: AsyncSession = Depends(get_db_session),
    user_claims: dict = Depends(require_auth([UserRoles.ADMIN])),
):
    result = await process_backfill_file(session, file)
    return {"status": HTTP_200_OK, "data": result}


@populate_database_route.get("/")
async def populate_database_by_exchange(
    exchange_code: str,
    session: AsyncSession = Depends(get_db_session),
    user_claims: dict = Depends(require_auth([])),
):
    exchange = await get_exchange_by_code(session, exchange_code)
    if exchange is None:
        raise HTTPException(status_code=404, detail="Exchange not found")

    instruments = await session.execute(
        select(
            Instrument.id,
            Instrument.symbol,
            ProviderInstrumentMapping.provider_instrument_search_code,
        )
        .join(
            ProviderInstrumentMapping,
            ProviderInstrumentMapping.instrument_id == Instrument.id,
        )
        .where(
            Instrument.exchange_id == exchange.id,
            ProviderInstrumentMapping.provider_instrument_search_code != None,
        )
    )
    instruments = instruments.all()

    print(instruments)

    for instrument_id, symbol, search_code in instruments:
        await populate_instrument_in_database(
            session, instrument_id, symbol, search_code
        )

    return {
        "status": HTTP_200_OK,
        "message": f"Database populated for exchange {exchange_code}",
    }


@populate_database_route.post("/create_price_history_records_for_future")
async def create_price_history_records_for_future(
    target_date: date,
    session: AsyncSession = Depends(get_db_session),
    user_claims: dict = Depends(require_auth([UserRoles.ADMIN])),
):
    exchange_data = await get_all_active_exchanges(session=session)
    data_creation = DataCreationService(session=session)

    for exchange in exchange_data:
        data_creation.add_exchange(exchange=exchange)

    await data_creation.start_data_creation(target_date=target_date)

    return {
        "status": HTTP_200_OK,
        "message": f"Price history records creation started for futures exchanges for date {target_date}",
    }
