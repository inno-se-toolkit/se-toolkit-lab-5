from fastapi import APIRouter, Depends, Query
from sqlmodel import select, func, col
from sqlmodel.ext.asyncio.session import AsyncSession
from ..database import get_session
from ..models.item import ItemRecord as Item
from ..models.interaction import InteractionLog
from ..auth import verify_api_key
router = APIRouter()
@router.get("/scores")
async def get_scores(lab: str = Query(...), session: AsyncSession = Depends(get_session)):
    lab_title = lab.replace("-", " ").title()
    lab_stmt = select(Item).where(col(Item.title).contains(lab_title), Item.type == "lab")
    lab_obj = (await session.exec(lab_stmt)).first()
    if not lab_obj: return []
    tasks_stmt = select(Item.id).where(Item.parent_id == lab_obj.id)
    task_ids = (await session.exec(tasks_stmt)).all()
    if not task_ids: return []
    stmt = (select(func.date(InteractionLog.created_at).label("date"), func.count(InteractionLog.id).label("submissions"))
        .where(InteractionLog.item_id.in_(task_ids)).group_by(func.date(InteractionLog.created_at)).order_by(func.date(InteractionLog.created_at)))
    results = (await session.exec(stmt)).all()
    return [{"date": str(row[0]), "submissions": row[1]} for row in results]
@router.get("/pass-rates")
async def get_pass_rates(lab: str = Query(...), session: AsyncSession = Depends(get_session)):
    lab_title = lab.replace("-", " ").title()
    lab_obj = (await session.exec(select(Item).where(col(Item.title).contains(lab_title), Item.type == "lab"))).first()
    if not lab_obj: return []
    stmt = (select(Item.title, func.avg(InteractionLog.score)).join(InteractionLog, InteractionLog.item_id == Item.id)
        .where(Item.parent_id == lab_obj.id).group_by(Item.id))
    results = (await session.exec(stmt)).all()
    return [{"task": row[0], "avg_score": round(float(row[1]), 2)} for row in results]
@router.get("/timeline")
async def get_timeline(lab: str = Query(...), session: AsyncSession = Depends(get_session)):
    return await get_scores(lab, session)
