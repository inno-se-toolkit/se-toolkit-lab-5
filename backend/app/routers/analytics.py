"""Router for analytics endpoints.

Each endpoint performs SQL aggregation queries on the interaction data
populated by the ETL pipeline. All endpoints require a `lab` query
parameter to filter results by lab (e.g., "lab-01").
"""

from typing import Any

from fastapi import APIRouter, Depends, Query
from sqlalchemy import case, func, distinct, cast, String
from sqlmodel import select
from sqlmodel.ext.asyncio.session import AsyncSession

from app.database import get_session
from app.models.item import ItemRecord
from app.models.interaction import InteractionLog
from app.models.learner import Learner

router = APIRouter()


async def _get_lab_and_task_ids(session: AsyncSession, lab_short_id: str) -> tuple[int | None, list[int]]:
    """Find lab item by short_id and return (lab_id, [task_ids]).
    
    The lab_short_id like "lab-04" should match title containing "Lab 04".
    """
    # Convert "lab-04" to "Lab 04" for title matching
    lab_title_pattern = f"Lab {lab_short_id.replace('lab-', '')}"
    
    # Find the lab
    lab_stmt = select(ItemRecord).where(
        ItemRecord.type == "lab",
        ItemRecord.title.ilike(f"%{lab_title_pattern}%")  # type: ignore[arg-type]
    )
    lab_result = await session.exec(lab_stmt)
    lab = lab_result.first()
    
    if not lab:
        return None, []
    
    # Find all tasks for this lab
    tasks_stmt = select(ItemRecord).where(
        ItemRecord.type == "task",
        ItemRecord.parent_id == lab.id
    )
    tasks_result = await session.exec(tasks_stmt)
    task_ids = [t.id for t in tasks_result]  # type: ignore[misc]
    
    return lab.id, task_ids


@router.get("/scores")
async def get_scores(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
) -> list[dict[str, Any]]:
    """Score distribution histogram for a given lab."""
    _, task_ids = await _get_lab_and_task_ids(session, lab)
    
    if not task_ids:
        return [
            {"bucket": "0-25", "count": 0},
            {"bucket": "26-50", "count": 0},
            {"bucket": "51-75", "count": 0},
            {"bucket": "76-100", "count": 0},
        ]
    
    # Query interactions with scores for this lab's tasks
    case_expr = case(
        (InteractionLog.score <= 25, "0-25"),  # type: ignore[arg-type]
        (InteractionLog.score <= 50, "26-50"),  # type: ignore[arg-type]
        (InteractionLog.score <= 75, "51-75"),  # type: ignore[arg-type]
        else_="76-100",
    )
    
    stmt = (
        select(
            cast(case_expr, String).label("bucket"),
            func.count().label("count"),
        )
        .where(
            InteractionLog.item_id.in_(task_ids),  # type: ignore[attr-defined]
            InteractionLog.score.isnot(None)  # type: ignore[attr-defined]
        )
        .group_by(cast(case_expr, String))
    )
    
    result = await session.exec(stmt)
    bucket_counts: dict[str, int] = {row.bucket: row.count for row in result}  # type: ignore[attr-defined]
    
    # Always return all four buckets
    return [
        {"bucket": "0-25", "count": bucket_counts.get("0-25", 0)},
        {"bucket": "26-50", "count": bucket_counts.get("26-50", 0)},
        {"bucket": "51-75", "count": bucket_counts.get("51-75", 0)},
        {"bucket": "76-100", "count": bucket_counts.get("76-100", 0)},
    ]


@router.get("/pass-rates")
async def get_pass_rates(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
) -> list[dict[str, Any]]:
    """Per-task pass rates for a given lab."""
    _, task_ids = await _get_lab_and_task_ids(session, lab)
    
    if not task_ids:
        return []
    
    # Get task titles
    tasks_stmt = select(ItemRecord).where(ItemRecord.id.in_(task_ids)).order_by(ItemRecord.title)  # type: ignore[attr-defined]
    tasks_result = await session.exec(tasks_stmt)
    tasks = {t.id: t.title for t in tasks_result}  # type: ignore[misc]
    
    # Query interactions grouped by task
    stmt = (
        select(
            InteractionLog.item_id,
            func.avg(InteractionLog.score).label("avg_score"),
            func.count().label("attempts"),
        )
        .where(InteractionLog.item_id.in_(task_ids))  # type: ignore[attr-defined]
        .group_by(InteractionLog.item_id)
    )
    
    result = await session.exec(stmt)
    
    response = []
    for task_id in sorted(tasks.keys()):
        task_avg = None
        task_attempts = 0
        for row in result:
            if row.item_id == task_id:  # type: ignore[attr-defined]
                task_avg = round(float(row.avg_score), 1) if row.avg_score else 0.0  # type: ignore[attr-defined]
                task_attempts = row.attempts  # type: ignore[attr-defined]
                break
        
        response.append({
            "task": tasks[task_id],
            "avg_score": task_avg if task_avg is not None else 0.0,
            "attempts": task_attempts,
        })
    
    return response


@router.get("/timeline")
async def get_timeline(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
) -> list[dict[str, Any]]:
    """Submissions per day for a given lab."""
    _, task_ids = await _get_lab_and_task_ids(session, lab)
    
    if not task_ids:
        return []
    
    stmt = (
        select(
            func.date(InteractionLog.created_at).label("date"),
            func.count().label("submissions"),
        )
        .where(InteractionLog.item_id.in_(task_ids))  # type: ignore[attr-defined]
        .group_by(func.date(InteractionLog.created_at))
        .order_by(func.date(InteractionLog.created_at))
    )
    
    result = await session.exec(stmt)
    
    return [
        {"date": str(row.date), "submissions": row.submissions}  # type: ignore[attr-defined]
        for row in result
    ]


@router.get("/groups")
async def get_groups(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
) -> list[dict[str, Any]]:
    """Per-group performance for a given lab."""
    _, task_ids = await _get_lab_and_task_ids(session, lab)
    
    if not task_ids:
        return []
    
    stmt = (
        select(
            Learner.student_group.label("group"),
            func.avg(InteractionLog.score).label("avg_score"),
            func.count(distinct(Learner.id)).label("students"),  # type: ignore[arg-type]
        )
        .join(Learner, InteractionLog.learner_id == Learner.id)
        .where(InteractionLog.item_id.in_(task_ids))  # type: ignore[attr-defined]
        .group_by(Learner.student_group)
        .order_by(Learner.student_group)
    )
    
    result = await session.exec(stmt)
    
    return [
        {
            "group": row.group,  # type: ignore[attr-defined]
            "avg_score": round(float(row.avg_score), 1) if row.avg_score else 0.0,  # type: ignore[attr-defined]
            "students": row.students,  # type: ignore[attr-defined]
        }
        for row in result
    ]
