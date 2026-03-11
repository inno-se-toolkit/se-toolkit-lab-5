from fastapi import APIRouter, Depends, Query
from sqlmodel import select, func
from sqlmodel.ext.asyncio.session import AsyncSession
from typing import List, Dict, Any
from app.database import get_session
from app.models.item import ItemRecord
from app.models.interaction import InteractionLog
from app.models.learner import Learner

router = APIRouter()

@router.get("/scores")
async def get_scores(
    lab: str = Query(..., description="Lab identifier, e.g. lab-04"),
    db: AsyncSession = Depends(get_session)
) -> List[Dict[str, Any]]:
    """
    Return score distribution in 4 buckets: 0-25, 26-50, 51-75, 76-100.
    Each bucket contains count of interactions with scores in that range.
    """
    # Convert lab param to title format: "lab-04" -> "Lab 04"
    lab_title = lab.replace('-', ' ').title()  # "lab-04" -> "Lab 04"

    # Find the lab item (parent) whose title contains the lab name
    result = await db.exec(
        select(ItemRecord).where(ItemRecord.title.ilike(f"%{lab_title}%")))
    lab_item = result.first()
    if not lab_item:
        return []

    result = await db.exec(select(ItemRecord).where(ItemRecord.parent_id == lab_item.id))
    tasks = result.all()
    task_ids = [t.id for t in tasks]
    if not task_ids:
        return []

    # Define buckets
    buckets = [
        (0, 25, "0-25"),
        (26, 50, "26-50"),
        (51, 75, "51-75"),
        (76, 100, "76-100"),
    ]

    output = []
    for low, high, label in buckets:
        count_result = await db.exec(
            select(func.count(InteractionLog.id))
            .where(InteractionLog.item_id.in_(task_ids))
            .where(InteractionLog.score >= low)
            .where(InteractionLog.score <= high)
        )
        count = count_result.one()
        output.append({"bucket": label, "count": count})
    return output

@router.get("/pass-rates")
async def get_pass_rates(
    lab: str = Query(..., description="Lab identifier, e.g. lab-04"),
    db: AsyncSession = Depends(get_session)
) -> List[Dict[str, Any]]:
    """
    Return per-task statistics: task title, average score (rounded to 1 decimal),
    and total number of attempts (interactions).
    """
    lab_title = lab.replace('-', ' ').title()
    result = await db.exec(
        select(ItemRecord).where(ItemRecord.title.ilike(f"%{lab_title}%")))
    lab_item = result.first()
    if not lab_item:
        return []

    result = await db.exec(select(ItemRecord).where(ItemRecord.parent_id == lab_item.id))
    tasks = result.all()

    output = []
    for task in tasks:
        stats_result = await db.exec(
            select(
                func.avg(InteractionLog.score).label("avg_score"),
                func.count(InteractionLog.id).label("attempts")
            )
            .where(InteractionLog.item_id == task.id)
        )
        stats = stats_result.one()
        output.append({
            "task": task.title,
            "avg_score": round(stats.avg_score or 0, 1),
            "attempts": stats.attempts
        })

    output.sort(key=lambda x: x["task"])
    return output


@router.get("/timeline")
async def get_timeline(
    lab: str = Query(..., description="Lab identifier, e.g. lab-04"),
    db: AsyncSession = Depends(get_session)
) -> List[Dict[str, Any]]:
    """
    Return number of submissions per day (by created_at date).
    """
    lab_title = lab.replace('-', ' ').title()
    result = await db.exec(
        select(ItemRecord).where(ItemRecord.title.ilike(f"%{lab_title}%")))
    lab_item = result.first()
    if not lab_item:
        return []

    result = await db.exec(select(ItemRecord).where(ItemRecord.parent_id == lab_item.id))
    tasks = result.all()
    task_ids = [t.id for t in tasks]
    if not task_ids:
        return []

    # Group by date (cast created_at to date)
    timeline_result = await  db.exec(
        select(
            func.date(InteractionLog.created_at).label("date"),
            func.count(InteractionLog.id).label("submissions")
        )
        .where(InteractionLog.item_id.in_(task_ids))
        .group_by(func.date(InteractionLog.created_at))
        .order_by(func.date(InteractionLog.created_at))
    )

    rows = timeline_result.all()
    return [{"date": str(r.date), "submissions": r.submissions} for r in rows]


@router.get("/groups")
async def get_groups(
    lab: str = Query(..., description="Lab identifier, e.g. lab-04"),
    db:AsyncSession = Depends(get_session)
) -> List[Dict[str, Any]]:
    """
    Return per-group performance: group name, average score (rounded to 1 decimal),
    and number of distinct students in that group.
    """
    lab_title = lab.replace('-', ' ').title()
    result = await db.exec(
        select(ItemRecord).where(ItemRecord.title.ilike(f"%{lab_title}%")))
    lab_item = result.first()
    if not lab_item:
        return []

    result = await db.exec(select(ItemRecord).where(ItemRecord.parent_id == lab_item.id))
    tasks = result.all()
    task_ids = [t.id for t in tasks]
    if not task_ids:
        return []

    # Join Interaction with Learner, group by student_group
    groups_result = await db.exec(
        select(
            Learner.student_group.label("group"),
            func.avg(InteractionLog.score).label("avg_score"),
            func.count(func.distinct(Learner.id)).label("students")
        )
        .join(InteractionLog, InteractionLog.learner_id == Learner.id)
        .where(InteractionLog.item_id.in_(task_ids))
        .group_by(Learner.student_group)
        .order_by(Learner.student_group)
    )
    rows = groups_result.all()
    return [
        {
            "group": r.group,
            "avg_score": round(r.avg_score or 0, 1),
            "students": r.students
        }
        for r in rows
    ]
