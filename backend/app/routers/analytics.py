from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import func, case, cast, Date, select
from sqlalchemy.ext.asyncio import AsyncSession
from typing import List
 
from app.db import get_session
from app.models.models import Item, InteractionLog, Learner
from app.models.schemas import ScoreBucket, TaskPassRate, TimelineEntry, GroupPerformance
 
router = APIRouter(prefix="/analytics", tags=["analytics"])
 
async def get_lab_task_ids(lab_param: str, session: AsyncSession) -> List[int]:
    """Вспомогательная функция для поиска ID задач конкретной лабы."""
    # Преобразуем "lab-04" -> "Lab 04"
    lab_title_part = lab_param.replace("-", " ").title()
 
    # Ищем айтем лабы
    lab_stmt = select(Item).where(Item.title.contains(lab_title_part), Item.type == "lab")
    result = await session.execute(lab_stmt)
    lab_item = result.scalars().first()
 
    if not lab_item:
        return []
 
    # Ищем все подзадачи этой лабы
    tasks_stmt = select(Item.id).where(Item.parent_id == lab_item.id)
    result = await session.execute(tasks_stmt)
    return result.scalars().all()
 
@router.get("/scores", response_model=List[ScoreBucket])
async def get_scores_histogram(lab: str, session: AsyncSession = Depends(get_session)):
    task_ids = await get_lab_task_ids(lab, session)
 
    buckets = ["0-25", "26-50", "51-75", "76-100"]
    if not task_ids:
        return [{"bucket": b, "count": 0} for b in buckets]
 
    stmt = (
        select(
            case(
                (InteractionLog.score <= 25, "0-25"),
                (InteractionLog.score <= 50, "26-50"),
                (InteractionLog.score <= 75, "51-75"),
                else_="76-100"
            ).label("bucket"),
            func.count(InteractionLog.id).label("count")
        )
        .where(InteractionLog.item_id.in_(task_ids))
        .group_by("bucket")
    )
 
    result = await session.execute(stmt)
    data = {row.bucket: row.count for row in result}
 
    return [{"bucket": b, "count": data.get(b, 0)} for b in buckets]
 
@router.get("/pass-rates", response_model=List[TaskPassRate])
async def get_pass_rates(lab: str, session: AsyncSession = Depends(get_session)):
    task_ids = await get_lab_task_ids(lab, session)
    if not task_ids:
        return []
 
    stmt = (
        select(
            Item.title.label("task"),
            func.round(cast(func.avg(InteractionLog.score), func.numeric), 1).label("avg_score"),
            func.count(InteractionLog.id).label("attempts")
        )
        .join(InteractionLog, InteractionLog.item_id == Item.id)
        .where(Item.id.in_(task_ids))
        .group_by(Item.title)
        .order_by(Item.title)
    )
 
    result = await session.execute(stmt)
    return result.mappings().all()
 
@router.get("/timeline", response_model=List[TimelineEntry])
async def get_timeline(lab: str, session: AsyncSession = Depends(get_session)):
    task_ids = await get_lab_task_ids(lab, session)
    if not task_ids:
        return []
 
    stmt = (
        select(
            cast(InteractionLog.created_at, Date).label("date"),
            func.count(InteractionLog.id).label("submissions")
        )
        .where(InteractionLog.item_id.in_(task_ids))
        .group_by(cast(InteractionLog.created_at, Date))
        .order_by("date")
    )
 
    result = await session.execute(stmt)
    return [{"date": str(row.date), "submissions": row.submissions} for row in result]
 
@router.get("/groups", response_model=List[GroupPerformance])
async def get_group_performance(lab: str, session: AsyncSession = Depends(get_session)):
    task_ids = await get_lab_task_ids(lab, session)
    if not task_ids:
        return []
 
    stmt = (
        select(
            Learner.student_group.label("group"),
            func.round(cast(func.avg(InteractionLog.score), func.numeric), 1).label("avg_score"),
            func.count(func.distinct(Learner.id)).label("students")
        )
        .join(InteractionLog, InteractionLog.learner_id == Learner.id)
        .where(InteractionLog.item_id.in_(task_ids))
        .group_by(Learner.student_group)
        .order_by(Learner.student_group)
    )
 
    result = await session.execute(stmt)
    return result.mappings().all()