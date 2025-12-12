from typing import List

from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from config.database_config import get_db_session
from features.auth.auth_service import require_auth
from features.sector import sector_service
from features.sector.sector_schema import SectorCreate, SectorInDb
from utils.common_constants import UserRoles

sector_router = APIRouter(
    prefix="/sector",
    tags=["sectors"],
)


@sector_router.post("/")
async def create_sector(
        create_sector: SectorCreate,
        user_claims: dict = Depends(require_auth([UserRoles.ADMIN])),
        session: AsyncSession = Depends(get_db_session),
) -> SectorInDb:
    sector = await sector_service.create_sector(
        session=session,
        sector_data=create_sector,
    )

    return sector


@sector_router.get("/{sector_id}")
async def get_sector(
        sector_id: int,
        user_claims: dict = Depends(require_auth([UserRoles.ADMIN])),
        session: AsyncSession = Depends(get_db_session),
) -> SectorInDb:
    sector = await sector_service.get_sector_by_id(
        session=session,
        sector_id=sector_id,
    )
    return sector


@sector_router.get("/")
async def list_sectors(
        user_claims: dict = Depends(require_auth()),
        session: AsyncSession = Depends(get_db_session),
) -> List[SectorInDb]:
    sectors = await sector_service.get_all_sectors(session=session)
    return sectors


@sector_router.put("/{sector_id}")
async def update_sector(
        sector_id: int,
        user_claims: dict = Depends(require_auth([UserRoles.ADMIN])),
        session: AsyncSession = Depends(get_db_session),
):
    sector = await sector_service.update_sector(
        session=session,
        sector_id=sector_id,
    )
    return sector
