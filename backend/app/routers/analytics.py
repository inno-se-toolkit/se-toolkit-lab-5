"""Router for analytics endpoints.

Each endpoint performs SQL aggregation queries on the interaction data
populated by the ETL pipeline. All endpoints require a `lab` query
parameter to filter results by lab (e.g., "lab-01").
"""

from fastapi import APIRouter, Depends, Query
from sqlalchemy import case, func
from sqlmodel import select
from sqlmodel.ext.asyncio.session import AsyncSession

from app.database import get_session
from app.models.item import ItemRecord
from app.models.interaction import InteractionLog
from app.models.learner import Learner

router = APIRouter()


def _normalize_lab_title(lab: str) -> str:
    """Convert lab slug like 'lab-04' to searchable title fragment 'Lab 04'."""
    return lab.replace("-", " ").title()


async def _get_lab_and_task_ids(session: AsyncSession, lab: str):
    """Find the lab item and IDs of all child task items."""
    lab_title = _normalize_lab_title(lab)

    result = await session.exec(
        select(ItemRecord).where(ItemRecord.title.contains(lab_title))
    )
    lab_item = result.first()

    if not lab_item:
        return None, []

    result = await session.exec(
        select(ItemRecord.id).where(ItemRecord.parent_id == lab_item.id)
    )
    task_ids = result.all()

    return lab_item, list(task_ids)


@router.get("/scores")
async def get_scores(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):
    """Score distribution histogram for a given lab."""
    _, task_ids = await _get_lab_and_task_ids(session, lab)

    buckets = [
        {"bucket": "0-25", "count": 0},
        {"bucket": "26-50", "count": 0},
        {"bucket": "51-75", "count": 0},
        {"bucket": "76-100", "count": 0},
    ]

    if not task_ids:
        return buckets

    bucket_case = case(
        (InteractionLog.score <= 25, "0-25"),
        (InteractionLog.score <= 50, "26-50"),
        (InteractionLog.score <= 75, "51-75"),
        else_="76-100",
    )

    result = await session.exec(
        select(
            bucket_case.label("bucket"),
            func.count().label("count"),
        )
        .where(
            InteractionLog.item_id.in_(task_ids),
            InteractionLog.score.is_not(None),
        )
        .group_by(bucket_case)
    )
    rows = result.all()

    counts = {row.bucket: row.count for row in rows}

    for bucket in buckets:
        bucket["count"] = counts.get(bucket["bucket"], 0)

    return buckets


@router.get("/pass-rates")
async def get_pass_rates(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):
    """Per-task pass rates for a given lab."""
    _, task_ids = await _get_lab_and_task_ids(session, lab)

    if not task_ids:
        return []

    result = await session.exec(
        select(
            ItemRecord.title.label("task"),
            func.round(func.avg(InteractionLog.score), 1).label("avg_score"),
            func.count(InteractionLog.id).label("attempts"),
        )
        .select_from(InteractionLog)
        .join(ItemRecord, InteractionLog.item_id == ItemRecord.id)
        .where(
            ItemRecord.id.in_(task_ids),
            InteractionLog.score.is_not(None),
        )
        .group_by(ItemRecord.id, ItemRecord.title)
        .order_by(ItemRecord.title)
    )
    rows = result.all()

    return [
        {
            "task": row.task,
            "avg_score": float(row.avg_score) if row.avg_score is not None else None,
            "attempts": row.attempts,
        }
        for row in rows
    ]


@router.get("/timeline")
async def get_timeline(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):
    """Submissions per day for a given lab."""
    _, task_ids = await _get_lab_and_task_ids(session, lab)

    if not task_ids:
        return []

    day_expr = func.date(InteractionLog.created_at)

    result = await session.exec(
        select(
            day_expr.label("date"),
            func.count(InteractionLog.id).label("submissions"),
        )
        .where(InteractionLog.item_id.in_(task_ids))
        .group_by(day_expr)
        .order_by(day_expr)
    )
    rows = result.all()

    return [
        {
            "date": row.date,
            "submissions": row.submissions,
        }
        for row in rows
    ]


@router.get("/groups")
async def get_groups(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):
    """Per-group performance for a given lab."""
    _, task_ids = await _get_lab_and_task_ids(session, lab)

    if not task_ids:
        return []

    result = await session.exec(
        select(
            Learner.student_group.label("group"),
            func.round(func.avg(InteractionLog.score), 1).label("avg_score"),
            func.count(func.distinct(Learner.id)).label("students"),
        )
        .select_from(InteractionLog)
        .join(Learner, Learner.id == InteractionLog.learner_id)
        .where(
            InteractionLog.item_id.in_(task_ids),
            InteractionLog.score.is_not(None),
        )
        .group_by(Learner.student_group)
        .order_by(Learner.student_group)
    )
    rows = result.all()

    return [
        {
            "group": row.group,
            "avg_score": float(row.avg_score) if row.avg_score is not None else None,
            "students": row.students,
        }
        for row in rows
    ]
