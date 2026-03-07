"""Pipeline router for ETL sync endpoint."""

from fastapi import APIRouter, Depends
from sqlmodel.ext.asyncio.session import AsyncSession

from app.database import get_session
from app.etl import sync


router = APIRouter(tags=["pipeline"])


@router.post("/sync")
async def pipeline_sync(session: AsyncSession = Depends(get_session)) -> dict:
    """Run the ETL pipeline to sync data from the autochecker API.

    Returns:
        dict: {"new_records": int, "total_records": int}
    """
    return await sync(session)