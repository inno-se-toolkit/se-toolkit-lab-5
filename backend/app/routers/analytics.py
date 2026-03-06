from fastapi import APIRouter, Depends
from sqlalchemy import select, func, cast, Float
from sqlalchemy.ext.asyncio import AsyncSession
from app.database import get_db 
from app.models.interaction import InteractionLog
from app.models.learner import Learner

router = APIRouter(prefix="/analytics", tags=["analytics"])

@router.get("/scores")
async def get_scores(db: AsyncSession = Depends(get_db)):
    stmt = select(InteractionLog.learner_id, func.avg(InteractionLog.score).label("average_score")).group_by(InteractionLog.learner_id)
    res = await db.execute(stmt)
    return [dict(row._mapping) for row in res]

@router.get("/pass-rates")
async def get_pass_rates(db: AsyncSession = Depends(get_db)):
    stmt = select(InteractionLog.item_id, (cast(func.sum(InteractionLog.checks_passed), Float) / func.nullif(func.sum(InteractionLog.checks_total), 0)).label("pass_rate")).group_by(InteractionLog.item_id)
    res = await db.execute(stmt)
    return [dict(row._mapping) for row in res]

@router.get("/timeline")
async def get_timeline(db: AsyncSession = Depends(get_db)):
    stmt = select(func.date(InteractionLog.submitted_at).label("date"), func.count().label("submissions")).group_by(func.date(InteractionLog.submitted_at)).order_by("date")
    res = await db.execute(stmt)
    return [dict(row._mapping) for row in res]

@router.get("/groups")
async def get_groups(db: AsyncSession = Depends(get_db)):
    stmt = select(Learner.group, func.avg(InteractionLog.score).label("average_score")).join(InteractionLog, Learner.id == InteractionLog.learner_id).group_by(Learner.group)
    res = await db.execute(stmt)
    return [dict(row._mapping) for row in res]
