"""Router for analytics endpoints.

Each endpoint performs SQL aggregation queries on the interaction data
populated by the ETL pipeline. All endpoints require a `lab` query
parameter to filter results by lab (e.g., "lab-01").
"""

from fastapi import APIRouter, Depends, Query
from sqlalchemy import case, func, select
from sqlmodel.ext.asyncio.session import AsyncSession

from app.database import get_session
from app.models.item import ItemRecord
from app.models.learner import Learner
from app.models.interaction import InteractionLog

router = APIRouter()


def _get_lab_title(lab: str) -> str:
    """Transform lab-04 → Lab 04."""
    return lab.replace("-", " ").title().replace("Lab  ", "Lab ")


async def _get_lab_item(session: AsyncSession, lab_title: str) -> ItemRecord | None:
    """Find the lab item by title."""
    lab_stmt = select(ItemRecord).where(
        ItemRecord.type == "lab",
        ItemRecord.title.ilike(f"%{lab_title}%")
    )
    lab_result = await session.execute(lab_stmt)
    return lab_result.scalar_one_or_none()


async def _get_task_ids(session: AsyncSession, lab_id: int) -> list[int]:
    """Get all task IDs that belong to a lab."""
    task_stmt = select(ItemRecord.id).where(
        ItemRecord.parent_id == lab_id,
        ItemRecord.type == "task"
    )
    task_result = await session.execute(task_stmt)
    return list(task_result.scalars().all())


@router.get("/scores")
async def get_scores(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):
    """Score distribution histogram for a given lab."""
    empty_result = [
        {"bucket": "0-25", "count": 0},
        {"bucket": "26-50", "count": 0},
        {"bucket": "51-75", "count": 0},
        {"bucket": "76-100", "count": 0},
    ]

    lab_title = _get_lab_title(lab)
    lab_item = await _get_lab_item(session, lab_title)
    if not lab_item:
        return empty_result

    task_ids = await _get_task_ids(session, lab_item.id)
    if not task_ids:
        return empty_result

    bucket_expr = case(
        (InteractionLog.score <= 25, "0-25"),
        (InteractionLog.score <= 50, "26-50"),
        (InteractionLog.score <= 75, "51-75"),
        (InteractionLog.score <= 100, "76-100"),
        else_="0-25"
    )

    stmt = select(
        bucket_expr.label("bucket"),
        func.count(InteractionLog.id).label("count")
    ).where(
        InteractionLog.item_id.in_(task_ids),
        InteractionLog.score.isnot(None)
    ).group_by(bucket_expr)

    result = await session.execute(stmt)
    bucket_counts = {row.bucket: row.count for row in result}

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
):
    """Per-task pass rates for a given lab."""
    lab_title = _get_lab_title(lab)
    lab_item = await _get_lab_item(session, lab_title)
    if not lab_item:
        return []

    task_stmt = select(ItemRecord).where(
        ItemRecord.parent_id == lab_item.id,
        ItemRecord.type == "task"
    ).order_by(ItemRecord.title)
    task_result = await session.execute(task_stmt)
    tasks = task_result.scalars().all()

    results = []
    for task in tasks:
        stats_stmt = select(
            func.round(func.avg(InteractionLog.score), 1).label("avg_score"),
            func.count(InteractionLog.id).label("attempts")
        ).where(
            InteractionLog.item_id == task.id,
            InteractionLog.score.isnot(None)
        )
        stats_result = await session.execute(stats_stmt)
        row = stats_result.first()
        if row:
            results.append({
                "task": task.title,
                "avg_score": float(row.avg_score) if row.avg_score is not None else 0.0,
                "attempts": row.attempts or 0,
            })

    return results


@router.get("/timeline")
async def get_timeline(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):
    """Submissions per day for a given lab."""
    lab_title = _get_lab_title(lab)
    lab_item = await _get_lab_item(session, lab_title)
    if not lab_item:
        return []

    task_ids = await _get_task_ids(session, lab_item.id)
    if not task_ids:
        return []

    # Use strftime for SQLite compatibility
    date_expr = func.strftime("%Y-%m-%d", InteractionLog.created_at)
    stmt = select(
        date_expr.label("date"),
        func.count(InteractionLog.id).label("submissions")
    ).where(
        InteractionLog.item_id.in_(task_ids)
    ).group_by(date_expr).order_by(date_expr)

    result = await session.execute(stmt)
    return [{"date": str(row.date), "submissions": row.submissions} for row in result]


@router.get("/groups")
async def get_groups(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):
    """Per-group performance for a given lab."""
    lab_title = _get_lab_title(lab)
    lab_item = await _get_lab_item(session, lab_title)
    if not lab_item:
        return []

    task_ids = await _get_task_ids(session, lab_item.id)
    if not task_ids:
        return []

    stmt = select(
        Learner.student_group.label("group"),
        func.round(func.avg(InteractionLog.score), 1).label("avg_score"),
        func.count(func.distinct(InteractionLog.learner_id)).label("students")
    ).join(
        Learner, InteractionLog.learner_id == Learner.id
    ).where(
        InteractionLog.item_id.in_(task_ids),
        InteractionLog.score.isnot(None)
    ).group_by(Learner.student_group).order_by(Learner.student_group)

    result = await session.execute(stmt)
    return [
        {
            "group": row.group,
            "avg_score": float(row.avg_score) if row.avg_score is not None else 0.0,
            "students": row.students,
        }
        for row in result
    ]