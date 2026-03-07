print("analytics.py loaded")
from fastapi import APIRouter, Depends, Query
from sqlmodel.ext.asyncio.session import AsyncSession
from sqlmodel import select, func
from app.database import get_session
from app.models import Item, InteractionLog, Learner
from typing import List, Dict, Any
from datetime import date

router = APIRouter(tags=["analytics"])


async def get_lab_tasks(lab: str, session: AsyncSession) -> List[Item]:
    lab_title = f"Lab {lab.split('-')[1]}"
    stmt = select(Item).where(Item.title.contains(lab_title), Item.type == "lab")
    result = await session.execute(stmt)
    lab_item = result.scalar_one_or_none()
    if not lab_item:
        return []
    stmt = select(Item).where(Item.parent_id == lab_item.id, Item.type == "task")
    result = await session.execute(stmt)
    return result.scalars().all()


@router.get("/scores")
async def get_scores(
    lab: str = Query(...),
    session: AsyncSession = Depends(get_session)
) -> List[Dict[str, Any]]:
    tasks = await get_lab_tasks(lab, session)
    if not tasks:
        return [{"bucket": "0-25", "count": 0}, {"bucket": "26-50", "count": 0},
                {"bucket": "51-75", "count": 0}, {"bucket": "76-100", "count": 0}]

    task_ids = [t.id for t in tasks]
    stmt = select(InteractionLog.score).where(InteractionLog.item_id.in_(task_ids))
    result = await session.execute(stmt)
    scores = [row[0] for row in result if row[0] is not None]

    buckets = {"0-25": 0, "26-50": 0, "51-75": 0, "76-100": 0}
    for s in scores:
        if s <= 25:
            buckets["0-25"] += 1
        elif s <= 50:
            buckets["26-50"] += 1
        elif s <= 75:
            buckets["51-75"] += 1
        else:
            buckets["76-100"] += 1

    return [{"bucket": k, "count": v} for k, v in buckets.items()]


@router.get("/pass-rates")
async def get_pass_rates(
    lab: str = Query(...),
    session: AsyncSession = Depends(get_session)
) -> List[Dict[str, Any]]:
    tasks = await get_lab_tasks(lab, session)
    if not tasks:
        return []

    result = []
    for task in tasks:
        stmt = select(
            func.avg(InteractionLog.score).label("avg_score"),
            func.count(InteractionLog.id).label("attempts")
        ).where(InteractionLog.item_id == task.id)
        row = (await session.execute(stmt)).first()
        result.append({
            "task": task.title,
            "avg_score": round(row.avg_score, 1) if row.avg_score else 0.0,
            "attempts": row.attempts
        })
    return result


@router.get("/timeline")
async def get_timeline(
    lab: str = Query(...),
    session: AsyncSession = Depends(get_session)
) -> List[Dict[str, Any]]:
    tasks = await get_lab_tasks(lab, session)
    if not tasks:
        return []

    task_ids = [t.id for t in tasks]
    stmt = select(
        func.cast(InteractionLog.created_at, date).label("date"),
        func.count(InteractionLog.id).label("submissions")
    ).where(InteractionLog.item_id.in_(task_ids)).group_by("date").order_by("date")
    result = await session.execute(stmt)
    return [{"date": str(row.date), "submissions": row.submissions} for row in result]


@router.get("/groups")
async def get_groups(
    lab: str = Query(...),
    session: AsyncSession = Depends(get_session)
) -> List[Dict[str, Any]]:
    tasks = await get_lab_tasks(lab, session)
    if not tasks:
        return []

    task_ids = [t.id for t in tasks]
    stmt = select(
        Learner.student_group.label("group"),
        func.avg(InteractionLog.score).label("avg_score"),
        func.count(func.distinct(Learner.id)).label("students")
    ).join(InteractionLog, Learner.id == InteractionLog.learner_id
    ).where(InteractionLog.item_id.in_(task_ids)
    ).group_by(Learner.student_group).order_by(Learner.student_group)
    result = await session.execute(stmt)
    return [{"group": row.group, "avg_score": round(row.avg_score, 1) if row.avg_score else 0.0,
             "students": row.students} for row in result]
